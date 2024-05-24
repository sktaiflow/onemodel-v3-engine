import pyarrow.parquet as pq
from abc import *
from abc import abstractmethod
from typing import (
    Union, 
    Dict, 
    Set, 
    List, 
    Callable, 
    Tuple, 
    Any
) 
import pyarrow as pa
from itertools import chain
import shutil
import psutil
import os
from torch.utils.data import (
    IterableDataset, 
    Dataset, 
    DataLoader
)
from datasets import (
    load_dataset, 
    Dataset, 
    DatasetDict,
    IterableDatasetDict
)


class AbstractPreprocessor(metaclass=ABCMeta):
    def __init__(self, args, **kwargs):
        self.args = args
    

    @classmethod
    def clean_cache_dir(cls, cache_dir):
        shutil.rmtree(cache_dir)

    @classmethod
    def load(cls, file_path:Union[str, List], split:str=None, stream:bool=True, keep_in_memory:bool=True, is_cache:bool=True, cache_dir:str='./.cache') -> IterableDataset:
        
        _, before_mem_usage_gb, before_mem_avail_gb = cls.get_ram_usage_percent()

        if isinstance(file_path, List):
            dataset = load_dataset("parquet", 
                                   data_files=file_path, 
                                   split=split, 
                                   keep_in_memory=keep_in_memory, 
                                   streaming=stream,
                                   cache_dir=cache_dir)

        elif isinstance(file_path, str):
            if os.path.isfile(file_path):
                dataset = load_dataset("parquet", 
                                       data_files=file_path, 
                                       split=split, 
                                       keep_in_memory=keep_in_memory, 
                                       streaming=stream,
                                       cache_dir=cache_dir)
            
            elif os.path.isdir(file_path):
                dataset = load_dataset(file_path, 
                                       streaming=stream,
                                       cache_dir=cache_dir)
            else:
                raise FileNotFoundError(f'{file_path} should be dir_path | file_path')
        else:
            raise TypeError(f'file_path should be in type str | List')
        
        if split is None:
            dataset = dataset['train']
        
        mem_usage_percent, mem_usage_gb, mem_avail_gb = cls.get_ram_usage_percent()
        memory_usage = {mem_usage_gb - before_mem_usage_gb}

        memory_info = {}
        memory_info['memory_usage'] = memory_usage
        memory_info['mem_usage_percent'] = mem_usage_percent

        if not is_cache:
            cls.clean_cache()

        return dataset, memory_info
            
    @classmethod
    def clean_cache(cls, dataset:Union[IterableDataset, Dataset]):
        dataset.cleanup_cache_files()


    @staticmethod
    def get_ram_usage_percent():
        """Returns the current system-wide RAM usage as a percentage."""

        mem = psutil.virtual_memory()

        return mem.percent, mem.used / (1024 ** 3), mem.available / (1024 ** 3)        
    
    @staticmethod
    def select_columns(dataset: Union[IterableDataset, Dataset, IterableDatasetDict], column_names:list=[]):
        if isinstance(dataset, IterableDataset) or isinstance(dataset, IterableDatasetDict):
            if isinstance(column_names, str):
                 column_names = column_names.split(',')
            elif isinstance(column_names, list):
                pass
            
            else:
                raise TypeError('column_names should be list or string type')
            
            dataset = dataset.select_columns(column_names)
                
        elif isinstance(dataset, Dataset):
            dataset = dataset['train'].select_columns([column_names])
            
        else:
            raise TypeError(f'dataset should be IterableDataset | Dataset')

        return dataset
    
    
    @abstractmethod
    def preprocess(self, item):
        f"""code for apply to map function"""

class StreamPreprocessor(AbstractPreprocessor):
    def __init__(self, args, **kwargs):        
        super().__init__(args)

    @classmethod
    def load(cls, file_path:Union[str, List], split:str=None, keep_in_memory:bool=True, is_cache:bool=True) -> IterableDataset:        
        stream = True
        dataset, memory_info = super(StreamPreprocessor, cls).load(
                file_path=file_path, 
                split=split, 
                stream=stream, 
                keep_in_memory=keep_in_memory,
                is_cache=is_cache
        )
        return dataset, memory_info
    
    
    @classmethod
    def shuffle(cls, dataset:IterableDataset, seed:int=777, buffer_size:int=1000) -> IterableDataset:
        return dataset.shuffle(seed=seed, buffer_size=buffer_size)

    @abstractmethod
    def preprocess(self, item):
        """ Implement preprocessing logic"""
    
    @classmethod
    def apply_maps(cls, dataset:Dataset, functions_list: List[Tuple[Callable[..., Any], bool]]) -> Dataset:
        """ instance method for apply list of functions"""
        for func, with_indices in functions_list:
            dataset = cls.apply_map(dataset=dataset, func=func, with_indices=with_indices)
        
        return dataset
    
    @classmethod
    def apply_map(cls, dataset: Dataset, func:Callable, with_indices: bool = True) -> Dataset:
        """ instance method for apply only one function"""
        dataset = dataset.map(func, with_indices=with_indices)
        return dataset
        