from pathlib import Path


class DirectoryReader:

    def __init__(self, directory_path: str):
        self.directory_path = Path(directory_path)

    def get_all_files(self):
        files = [str(f) for f in self.directory_path.iterdir() if f.is_file()]
        # print(files)
        return files

    def get_file_by_name(self, name: str):
        print(name," calling glob")
        return self.directory_path.glob(f"*{name}*")


if __name__ == "__main__":
    dirReader = DirectoryReader("../data_source_configs")
    dir = dirReader.get_all_files()
    print(f"Number of files: {len(dir)}")
