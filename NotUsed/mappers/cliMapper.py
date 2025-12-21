from dataclasses import dataclass
from typing import List

from readers.yaml_reader import YamlReader
from utils.dictToClass import dict_to_class
from utils.processRunner import ProcessRunner


@dataclass
class CommandDto:
    executable: str
    args: List[str]
    env: dict[str, str]

@dataclass
class CliDTO:
    command: CommandDto

class CliMapper:
    def __init__(self, tool_conf):
        print("running cli mapper")


        self.tool_conf = tool_conf
        self.commandCli = dict_to_class(tool_conf["command"], CommandDto)
        print(f"tool conf: {self.tool_conf}")
        self.run_cli()

    def run_cli(self):
        print("running cli mapper")
        runner = ProcessRunner(working_dir="./data_source_configs")
        pwd = runner.run(["pwd"])
        print(f"current WD {pwd.stdout}")
        if self.commandCli.env is not None:
            runner.set_env_vars(self.commandCli.env)
        # cmd=["pwd"]
        cmd = self.commandCli.args
        print(f"running command {cmd}")
        cmd_out = runner.run(cmd)
        print(cmd_out.stdout)
        print(f"error {cmd_out.stderr}")


if __name__ == "__main__":
    cli = YamlReader.get_yaml_content("../../data_source_configs/osm.yaml")["load"][0]["tool"]
    CliMapper(cli)
    print(cli)