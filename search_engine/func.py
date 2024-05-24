from typing import Any, Dict

def search(
        client:object, 
        query:dict, 
        index_name:str) -> Dict[str, Any]:
    
    response = client.search(
        body = query,
        index = index_name
    )
    return response