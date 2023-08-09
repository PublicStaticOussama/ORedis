import redis
import time
import json
import inspect
from redis import Connection
from redis.commands.search.field import (
    GeoField,
    NumericField,
    TagField,
    TextField,
    VectorField,
)
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import NumericFilter, Query
import uuid

def uuid_hex():
    return uuid.uuid4().hex

def waitForIndex(env, idx, timeout=None):
    delay = 0.1
    while True:
        res = env.execute_command("FT.INFO", idx)
        try:
            if int(res[res.index("indexing") + 1]) == 0:
                break
        except ValueError:
            break
        except AttributeError:
            try:
                if int(res["indexing"]) == 0:
                    break
            except ValueError:
                break

        time.sleep(delay)
        if timeout is not None:
            timeout -= delay
            if timeout <= 0:
                break

class ORedis:
    connection: Connection = None
    def __init__(self, host="localhost", port=6379, db=0):
        ORedis.connection = redis.Redis(host=host, port=port, db=db)
        self.connection = ORedis.connection
        self.connection.flushdb()

    @staticmethod
    def getConnection(host, port, db):
        if ORedis.connection is not None:
            return ORedis.connection
        ORedis.connection = redis.Redis(host=host, port=port, db=db)
        ORedis.connection.flushdb()
        return ORedis.connection

def ORedisSchema(cls):
    cls.subclass_name: str = cls.__name__
    cls.prefix = f"{cls.subclass_name.lower()}:"
    cls.index_name = cls.subclass_name.capitalize()
    definition = IndexDefinition(prefix=[cls.prefix], index_type=IndexType.HASH)
    
    instance = cls()

    schema = ()
    field_names = vars(instance).items()
    for fieldname, value in field_names:
        if issubclass(type(value), int):
            schema = schema + (NumericField(fieldname),)
        elif issubclass(type(value), str):
            schema = schema + (TextField(fieldname),)
        else:
            schema = schema + (TextField(fieldname),)

    try:
        cls.connection.ft(cls.index_name).create_index(schema, definition=definition)
        waitForIndex(cls.connection, cls.index_name)
    except Exception as e:
        print(e)

    def toString(self):
        return json.dumps(self.__dict__, indent=4)

    cls.__str__ = toString
    cls.__repr__ = toString

    def create(doc_dict):
        inst = cls()
        setattr(inst, "_id", uuid_hex())
        for fieldname, default_val in field_names:
            if fieldname in doc_dict:
                val = doc_dict[fieldname]
                setattr(inst, fieldname, int(val) if issubclass(type(default_val), int) else val)
        
        return inst
    
    cls.create = create
    
    def find(query):
        q_arr = []
        for field, value in query.items():
            if issubclass(type(value), int):
                q_arr.append(f"@{field}:[{str(value)} {str(value)}]")
            elif issubclass(type(value), str):
                q_arr.append(f"@{field}:{value}")
            else:
                q_arr.append(f"@{field}:{value}")
        res = cls.connection.ft(cls.index_name).search(Query(" ".join(q_arr) if bool(q_arr) else "*"))
        arr = []
        for doc in res.docs:
            arr.append(cls.create(doc.__dict__))
        
        return arr 

    cls.find = find

    def save(self):
        cls.connection.hset(f"{cls.prefix}{self._id}", mapping=self.__dict__)
        return self

    cls.save = save
    
    return cls

class Schema(ORedis):
    def __init__(self):
        self._id = uuid_hex()

