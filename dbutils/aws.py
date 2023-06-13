
import os
from fabric import task, Connection, SerialGroup, ThreadingGroup
from invoke import run as local
from ipdb import set_trace
import json, time

def aws_has_tag(res, key, value):
    for tag in res.get("Tags", []):
        if tag["Key"] == key and tag["Value"] == value:
            return True
    return False

def aws_tag_spec(restype, **tags):
    tagstr = ",".join("{Key=" + key + ",Value=" + value + "}" for (key,value) in tags.items())
    return f'ResourceType={restype},Tags=[{tagstr}]'

class AWSClient:
    def __init__(self, ctx, region="us-west-2", profile="dagknows"):
        self.ctx = ctx
        self.region = region
        self.profile = profile

    def run(self, cmd, *subcmds, **options):
        s2 = " ".join(subcmds)
        argstr = " ".join([f"{k} '{v}'" for k,v in options.items()])
        cmdstr = f"aws --profile={self.profile} --region={self.region} {cmd} {s2} {argstr}"
        try:
            res = local(cmdstr)
        except Exception as e:
            print("Exception calling command '{cmdstr}': ", e)
            return None
        if res.failed:
            print("Command '{cmdstr}' failed: ", e)
            return None
        if res.stdout.strip(): res = json.loads(res.stdout.strip())
        return res

    def ensure_elastic_ip(self, ipname):
        pairs = self.run("ec2", "describe-addresses").get("Addresses", [])
        pairs = [p for p in pairs if aws_has_tag(p, "Name", ipname)]
        if pairs: return pairs[0], False
        pair = self.run("ec2", "allocate-addresses", **{
            "--tag-specification": aws_tag_spec("address", **{ "Name": ipname }),
        })
        return pair, True

    def ensure_key_pair(self, name):
        keyfile = f"./keys/{name}.pem"
        exists= os.path.isfile(keyfile)
        if exists:
            pairs = self.run("ec2", "describe-key-pairs")["KeyPairs"]
            thepair = [p for p in pairs if p.get("KeyName") == name]
            exists = len(thepair) > 0

        if not exists:
            # delete first in case
            self.run("ec2", "delete-key-pair", **{ "--key-name":  name, })
            res = self.run("ec2", "create-key-pair", **{
                "--key-name": name,
            })
            pemdata = res["KeyMaterial"]
            with open(keyfile, "w") as kf:
                kf.write(pemdata)
        return True

    def ensure_sec_group_connectivity(self, sec_group_id, newports=None):
        """ Ensures we have connectivity to this sec group on the right protocols """
        newports = newports or [22, 80, 443]
        all_sgs =  self.run("ec2", "describe-security-groups")["SecurityGroups"]
        sgs = [sg for sg in all_sgs if sg["GroupId"] == sec_group_id]
        if not sgs:
            raise Exception(f"You may have deleted the security group {sec_group_id}")
        sg = sgs[0]
        if not aws_has_tag(sg, "IngressInited", "True"):
            # Setup inbound rules too
            print("setting inbound access for https and ssh")
            existing_ports = set(ipperm["FromPort"] for ipperm in sg.get("IpPermissions", []) if "FromPort" in ipperm)
            for port in newports:
                if port not in existing_ports:
                    print("Enabling inbound tcp port: ", port)
                    self.run("ec2", "authorize-security-group-ingress", **{
                        "--group-id": sg["GroupId"],
                        "--protocol": "tcp",
                        "--port": port,
                        "--cidr": "0.0.0.0/0",
                    })

                    if False and port != 22:
                        # Also enable ipv6
                        ec2("authorize-security-group-ingress", **{
                            "--group-id": sg["GroupId"],
                            "--protocol": "tcp",
                            "--port": port,
                            "--cidr": "::/0",
                        })


    def ensure_instance(self, filterfunc, creation_config):
        """ Ensures we have an instance that matches a given condition and if not creates it. """
        checker = lambda x: filterfunc(x) and x.get("State", {}).get("Name", "").lower() != "terminated"
        inst = self.find_instance(checker)
        newcreated = inst is None
        if not inst:
            imageid = creation_config["--image-id"]
            imageinfo = self.run("ec2", "describe-images", **{
                "--image-id": imageid
            })["Images"]
            devname = imageinfo[0]["BlockDeviceMappings"][0]["DeviceName"]
            volsize = creation_config.get("VolumeSize", 100)
            creation_config.update(**{
                "--block-device-mapping": f"DeviceName={devname},Ebs={{VolumeSize={volsize}}}"
            })
            result = self.run("ec2", "run-instances", **creation_config)
            inst = result["Instances"][0]
            inst_state = inst.get("State", {}).get("Name", "")
            while inst_state.lower() != "running":
                print("Waiting for instance to get to running state...")
                time.sleep(3)
                inst = self.find_instance(checker)
                inst_state = inst.get("State", {}).get("Name", "")

        inst_state = inst.get("State", {}).get("Name", "")
        if inst_state.lower() != "running":
            print("Instance state is not 'running'.  Wait for a while or terminate it")
        return inst, newcreated

    def find_instance(self, matchfunc):
        """ Finds the first instance that matches the given criteria. """
        instances = self.run("ec2", "describe-instances")
        reservations = instances["Reservations"]
        for res in reservations:
            for inst in res["Instances"]:
                if matchfunc(inst):
                    return inst
        return None
