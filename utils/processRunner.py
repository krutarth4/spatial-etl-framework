import os
import shutil
import subprocess
import logging
from pathlib import Path
from typing import List, Optional

from IPython.utils.capture import capture_output


class ProcessRunner:
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

    def __init__(self, working_dir: Optional[str] = None):
        self.working_dir = Path(working_dir) if working_dir else None
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

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
            print(f"Tool '{tool_name}' found at: {path}")
            return True
        else:
            self.logger.error(f"❌ Tool '{tool_name}' not found in system PATH.")
            print(f"Tool '{tool_name}' not found in system PATH.")
            return False

    def _clean_command(self, command: List[str]) -> List[str]:
        """Remove empty or whitespace-only arguments and strip spaces."""
        clean_cmd = [arg.strip() for arg in command if arg and arg.strip()]
        return clean_cmd

    def run(self, command: List[str], check: bool = False) -> "ProcessRunner.Result":
        """
        Runs a shell command safely and returns output.
        Args:
            command (List[str]): Command and arguments (e.g. ['ls', '-la'])
            check (bool): Raise exception if command fails.
        """
        self.logger.info(f"Running command: {' '.join(command)}")
        print("running process")
        if not command:
            raise ValueError("Command cannot be empty.")
        command = self._clean_command(command)
        tool = command[0]
        if not self._check_tool_exists(tool):
            return self.Result(tool, "", f"Tool '{tool}' not found.", returncode=127)

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

    def set_env_vars(self , env: dict[str, str]):
        """Set environment variables."""
        for k, v in env.items():
            os.environ[k] = v
            print(f"Set environment variable '{k}'")
        print("setting env variables ")
if __name__ == "__main__":
    # @password
    os.environ["PGPASSWORD"] = "admin123"

    runner = ProcessRunner(working_dir="../data_source_configs" )

    cmd=["osm2pgsql",
        "-c", "-d", "osm_berlin",
        "-H", "localhost",
        "-P", "5433",
        "-U", "postgres",
        "-S ../default.style",
        "-r", "pbf",
        "../process_single_file/raw/berlin.osm.pbf", "-v"]
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