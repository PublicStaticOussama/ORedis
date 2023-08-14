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

def resolve_bool(st):
    if type(st) != str and type(st) != bool:
        print("this one ???")
        raise Exception("Error: invalid boolean equivalent")
    if type(st) == bool:
        return st
    if st == "True" or st == "true":
        return True
    elif st == "False" or st == "false":
        return False
    else:
        raise Exception("Error: invalid boolean equivalent")

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
    def __init__(self, host="localhost", port=6379, db=0, flush=False):
        ORedis.connection = redis.Redis(host=host, port=port, db=db)
        self.connection = ORedis.connection
        if flush:
            self.connection.flushdb()

    @staticmethod
    def getConnection(host, port, db, flush=False):
        if ORedis.connection is not None:
            return ORedis.connection
        ORedis.connection = redis.Redis(host=host, port=port, db=db)
        if flush:
            ORedis.connection.flushdb()
        return ORedis.connection

def ORedisSchema(cls):
    if not hasattr(cls, 'connection'):
        raise Exception("Error: a connection to redis-stack has to be established before using an ORedis schema")
    if cls.connection is None:
        raise Exception("Error: the connection to redis-stack has to be established before defining an ORedis schema")
    cls.subclass_name: str = cls.__name__
    cls.prefix = f"{cls.subclass_name.lower()}:"
    cls.index_name = cls.subclass_name.capitalize()
    definition = IndexDefinition(prefix=[cls.prefix], index_type=IndexType.HASH)
    
    instance = cls()

    schema = ()
    field_names = vars(instance).items()
    for fieldname, value in field_names:
        if issubclass(type(value), bool):
            schema = schema + (TextField(fieldname),)
        elif issubclass(type(value), int):
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

    original_init = cls.__init__

    def new(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        setattr(self, "_id", uuid_hex())
        for fieldname, default_val in field_names:
            val = getattr(self, fieldname)
            cast_val = val
            if issubclass(type(default_val), bool): # this if HAS to come before the int case, because
                cast_val = resolve_bool(val)
            elif issubclass(type(default_val), float):
                cast_val = float(val) 
            elif issubclass(type(default_val), int):
                cast_val = int(val) 
            else:
                cast_val = str(val)
            setattr(self, fieldname, cast_val)


    cls.__init__ = new

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
                cast_val = val
                if issubclass(type(default_val), bool): # this if HAS to come before the int case, because
                    cast_val = resolve_bool(val)
                elif issubclass(type(default_val), float):
                    cast_val = float(val) 
                elif issubclass(type(default_val), int):
                    cast_val = int(val) 
                else:
                    cast_val = str(val)
                setattr(inst, fieldname, cast_val)
        
        return inst
    
    cls.create = create
    
    def find(query):
        q_arr = []
        for field, value in query.items():
            if field in field_names:
                if issubclass(type(field_names[field]), bool):
                    q_arr.append(f"@{field}:{str(value)}")
                elif issubclass(type(field_names[field]), float):
                    q_arr.append(f"@{field}:[{str(value)} {str(value)}]")
                elif issubclass(type(field_names[field]), int):
                    q_arr.append(f"@{field}:[{str(value)} {str(value)}]")
                elif issubclass(type(field_names[field]), str):
                    q_arr.append(f"@{field}:{value}")
                else:
                    q_arr.append(f"@{field}:{str(value)}")
        q_str = " ".join(q_arr) if bool(q_arr) else "*"
        res = cls.connection.ft(cls.index_name).search(Query(q_str))
        arr = []
        for doc in res.docs:
            arr.append(cls.create(doc.__dict__))
        
        return arr 

    cls.find = find

    def findOne(query) -> cls:
        q_arr = []
        for field, value in query.items():
            if field in field_names:
                if issubclass(type(field_names[field]), bool):
                    q_arr.append(f"@{field}:{str(value)}")
                elif issubclass(type(field_names[field]), float):
                    q_arr.append(f"@{field}:[{str(value)} {str(value)}]")
                elif issubclass(type(field_names[field]), int):
                    q_arr.append(f"@{field}:[{str(value)} {str(value)}]")
                elif issubclass(type(field_names[field]), str):
                    q_arr.append(f"@{field}:{value}")
                else:
                    q_arr.append(f"@{field}:{str(value)}")
        q_str = " ".join(q_arr) if bool(q_arr) else "*"
        res = cls.connection.ft(cls.index_name).search(Query(q_str).paging(0, 1))
        one = None
        for doc in res.docs:
            one = cls.create(doc.__dict__)
        
        return one
    
    cls.findOne: cls = findOne

    def insert(bulk):
        pipe: Connection = cls.connection.pipeline()
        fails = 0
        for doc in bulk:
            try:
                doc['_id'] = uuid_hex()
                for field in doc:
                    val = doc[field]
                    cast_val = val
                    if issubclass(type(doc[field]), bool):
                        cast_val = str(val)
                    else:
                        pass
                    doc[field] = cast_val
                pipe.hset(f"{cls.prefix}{doc['_id']}", mapping=doc)
            except Exception as e:
                fails += 1
                print("Failed at this category:", doc["name"])
                print("with error:", e)

        pipe.execute()
        if fails > 0:
            print(fails)
            print("is the number of fails equal to bluk size ?", "Yes" if fails == len(bulk) else "No !")

    cls.insert = insert

    def save(self):
        self_dict = self.__dict__
        for field in self_dict:
            val = self_dict[field]
            cast_val = val
            if issubclass(type(self_dict[field]), bool):
                cast_val = str(val)
            else:
                pass
            self_dict[field] = cast_val
        cls.connection.hset(f"{cls.prefix}{self._id}", mapping=self_dict)

        return self

    cls.save = save
    
    return cls

class Schema(ORedis):
    # Schema.__init__ has to be defined in Schema because it has to replace ORedis.__init__
    def __init__(self):
        # self._id = uuid_hex()
        pass

    def save(self):
        pass

    @classmethod
    def create(cls, doc_dict):
        pass

    @classmethod
    def find(cls, query):
        pass

    @classmethod
    def findOne(cls, query):
        pass

    @classmethod
    def insert(cls, bulk):
        pass

