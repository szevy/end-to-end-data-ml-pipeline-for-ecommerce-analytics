import pandas as pd
import dagster as dg
from upath import UPath

class PandasDictIOManager(dg.UPathIOManager):
    """
    An I/O Manager that handles saving and loading a dictionary of pandas DataFrames.
    Each DataFrame in the dictionary will be saved as a separate Parquet file
    within a subdirectory named after the asset key.
    """
    
    def __init__(self, base_path: str):
        super().__init__(base_path=UPath(base_path))

    def dump_to_path(self, context: dg.OutputContext, obj: dict, path: UPath) -> None:
        """Save a dictionary of DataFrames to separate Parquet files."""
        context.log.info(f"Saving dictionary of DataFrames to: {path}")
        
        if not isinstance(obj, dict):
            raise TypeError(f"Expected a dict of DataFrames, but got {type(obj)}")
        
        # Create directory
        path.mkdir(parents=True, exist_ok=True)
        
        if not obj:
            context.log.warning("Empty dictionary provided, no files to save")
            return
        
        # Save each DataFrame as a separate Parquet file
        for df_name, df in obj.items():
            if not isinstance(df, pd.DataFrame):
                raise TypeError(f"Expected pandas DataFrame for key '{df_name}', got {type(df)}")
            
            file_path = path / f"{df_name}.parquet"
            context.log.info(f"Saving '{df_name}' ({len(df)} rows) to {file_path}")
            df.to_parquet(file_path, index=False)

    def load_from_path(self, context: dg.InputContext, path: UPath) -> dict:
        """Load a dictionary of DataFrames from Parquet files."""
        context.log.info(f"Loading DataFrames from: {path}")
        
        if not path.exists() or not path.is_dir():
            raise FileNotFoundError(f"Directory {path} does not exist")
        
        loaded_dfs = {}
        
        # Load all Parquet files in the directory
        for file_path in path.iterdir():
            if file_path.suffix == ".parquet":
                df_name = file_path.stem
                context.log.info(f"Loading '{df_name}' from {file_path}")
                loaded_dfs[df_name] = pd.read_parquet(file_path)
            else:
                context.log.debug(f"Skipping non-Parquet file: {file_path}")
        
        if not loaded_dfs:
            context.log.warning(f"No Parquet files found in {path}")
        
        return loaded_dfs
