from bson.objectid import ObjectId
from typing import Union


def serialize(document: Union[dict, list]) -> Union[dict, list]:
    """
    Serialize a MongoDB document or a list of documents by converting ObjectId to string recursively
    and translating `_id` to `id`.
    """
    if isinstance(document, list):
        return [serialize(item) for item in document]

    if not document or not isinstance(document, dict):
        return document

    serialized = {}
    for key, value in document.items():
        if key == "_id":
            serialized["id"] = str(value)
        elif isinstance(value, ObjectId):
            serialized[key] = str(value)
        elif isinstance(value, dict):
            serialized[key] = serialize(value)
        elif isinstance(value, list):
            serialized[key] = [
                serialize(item) if isinstance(item, dict) else item for item in value
            ]
        else:
            serialized[key] = value
    return serialized


def deserialize(data: Union[dict, list]) -> Union[dict, list]:
    """
    Deserialize a dictionary or a list of dictionaries by converting string IDs to ObjectId recursively
    """
    if isinstance(data, list):
        return [deserialize(item) for item in data]

    if not data or not isinstance(data, dict):
        return data

    deserialized = {}
    for key, value in data.items():
        if key == "id":
            deserialized["_id"] = ObjectId(value)
        elif isinstance(value, dict):
            deserialized[key] = deserialize(value)
        elif isinstance(value, list):
            deserialized[key] = [
                deserialize(item) if isinstance(item, dict) else item for item in value
            ]
        else:
            deserialized[key] = value
    return deserialized
