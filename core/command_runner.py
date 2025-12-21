import os
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional

from log_manager.logger_manager import LoggerManager


class CommandRunner:
    """
    Utility class to run terminal commands safely and capture their output.

    Example:
        runner = ProcessRunner(working_dir="data")
        result = runner.run(["osm2pgsql", "-d", "osm_berlin", "berlin.osm.pbf"])
        if result.success:
            print("Import successful ✅")
        else:
            print("Import failed ❌", result.error)
    """

    def __init__(self, working_dir: Optional[str] = "../"):
        self.working_dir = Path(working_dir) if working_dir else None
        self.logger = LoggerManager(type(self).__name__)
        # self.logger.setLevel(logging.INFO)

    class Result:
        def __init__(self, command: str, stdout: str, stderr: str, returncode: int):
            self.command = command
            self.stdout = stdout
            self.stderr = stderr
            self.returncode = returncode

        @property
        def success(self):
            return self.returncode == 0

        @property
        def error(self):
            return None if self.success else self.stderr.strip()

    def _check_tool_exists(self, tool_name: str) -> bool:
        """Check if the command/tool exists in PATH using shutil.which."""
        path = shutil.which(tool_name)
        if path:
            self.logger.debug(f"Tool '{tool_name}' found at: {path}")
            self.logger.info(f"Tool '{tool_name}' found at: {path}")
            return True
        else:
            self.logger.error(f"❌ Tool '{tool_name}' not found in system PATH.")
            self.logger.warning(f"Install the tool and try again.")
            return False

    def _clean_command(self, command: List) -> List[str]:
        """Remove empty or whitespace-only arguments and strip spaces."""
        clean_cmd = [str(arg).strip() for arg in command if arg and str(arg).strip()]
        return clean_cmd

    def run(self, command: List, env_dict: dict, check: bool = True) -> "CommandRunner.Result":
        # print(f"Running command: {' '.join(command)}")

        if not command:
            raise ValueError("Command cannot be empty.")

        command = self._clean_command(command)
        self.logger.info(f"Running command: {' '.join(command)}")

        tool = command[0]
        if not self._check_tool_exists(tool):
            return self.Result(tool, "", f"Tool '{tool}' not found.", returncode=127)

        # apply env vars
        if env_dict:
            self.set_env_vars(env_dict)

        process = subprocess.Popen(
            command,
            cwd=self.working_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # line-buffered
        )

        stdout_lines = []
        stderr_lines = []

        # stream logs live
        for line in process.stdout:
            stdout_lines.append(line)
            self.logger.warning(line.strip())

        for line in process.stderr:
            stderr_lines.append(line)
            self.logger.error(line.strip())

        returncode = process.wait()

        if check and returncode != 0:
            raise subprocess.CalledProcessError(returncode, command)
        return self.Result(
            " ".join(command),
            "".join(stdout_lines),
            "".join(stderr_lines),
            returncode)

    def run_block(self, command: List[str], env_dict: dict, check: bool = True) -> "ProcessRunner.Result":
        """
        Runs a shell command safely and returns output.
        Args:
            command (List[str]): Command and arguments (e.g. ['ls', '-la'])
            check (bool): Raise exception if command fails.
        """
        self.logger.info(f"Running command: {' '.join(command)}")

        if not command:
            raise ValueError("Command cannot be empty.")
        command = self._clean_command(command)
        print(f"after clean command {command}")
        tool = command[0]
        if not self._check_tool_exists(tool):
            return self.Result(tool, "", f"Tool '{tool}' not found.", returncode=127)

        # set the env variables directly in the
        if len(env_dict) != 0 and env_dict is not None:
            self.set_env_vars(env_dict)

        try:
            result = subprocess.run(
                command,
                cwd=self.working_dir,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=check,
            )
            self.logger.info("✅ Command completed successfully.")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"❌ Command failed with code {e.returncode}")
            return self.Result(" ".join(command), e.stdout or "", e.stderr or str
            (e), e.returncode)

        return self.Result(" ".join(command), result.stdout, result.stderr, result.returncode)

    def set_env_vars(self, env: dict[str, str]):
        """Set environment variables."""
        for k, v in env.items():
            os.environ[k] = v
            self.logger.info(f"Set environment variable '{k}'")
        self.logger.info(f"Environment variable set Successfully")


if __name__ == "__main__":
    # @password
    os.environ["PGPASSWORD"] = "admin123"

    runner = CommandRunner(working_dir="../")
    # osm2pgrouting - f. / raw / map_extract.osm - d
    # osm_bbox_berlin - U
    # postgres - W
    # admin123 - p
    # 5433 - c
    # mapconfig.xml - -prefix
    # routing - -tags - -addnodes - -schema
    # pgrouting
    cmd = ["osm2pgrouting",
           "-f", "./raw/ernst_extract.osm", "-d", "test_runner",
           "-p", "5432",
           "-U", "postgres",
           "-W", "admin123",
           "-c", "mapconfig.xml",
           "--schema", "public"]
    # osm2pgsql -c -d osm_berlin -H localhost -P 5433 -U postgres -S ../../default.style -r "pbf" berlin.osm.pbf -W

    result = runner.run(cmd)

    if result.success:
        print("✅ OSM import completed successfully!")
    else:
        print("❌ Error:", result.error)
    # runner = ProcessRunner()
    # runner = ProcessRunner()
    # result = runner.run(["echo ", "Hello from OSM pipeline!"])

    print(result.stdout)
