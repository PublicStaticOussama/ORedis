##########################################################################
##########################################################################
# TODO: exec asDict field results shoulb be casted to their correct types
##########################################################################
##########################################################################


import redis as red
import redis.asyncio as redis
import time
from datetime import datetime
import json
import inspect
import traceback
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
import redis.commands.search.aggregation as aggregations
import redis.commands.search.reducers as reducers
import uuid

def get_current_timestamp():
    current = (datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()
    return int(current)

def find(condition, iterable):
    return next((item for item in iterable if condition(item)), None)

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
            # traceback_info = traceback.format_exc()
            # print(" +", traceback_info)
            break
        except AttributeError:
            # traceback_info = traceback.format_exc()
            # print(" +", traceback_info)
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
    sync: Connection = None
    def __init__(self, host="localhost", port=6379, db=0, flush=False):
        ORedis.host = host
        ORedis.port = port
        ORedis.db = db
        ORedis.connection = redis.Redis(host=host, port=port, db=db)
        self.connection = ORedis.connection
        ORedis.sync = red.Redis(host=host, port=port, db=db)
        self.sync = red.Redis(host=host, port=port, db=db)
        # self.connection.ft().aggregate()
        # if flush:
        #     await self.connection.flushdb()

    async def flush(self):
        return await self.connection.flushdb()
        
    @staticmethod
    async def getConnection(host, port, db, flush=False):
        if ORedis.connection is not None:
            return ORedis.connection
        ORedis.connection = redis.Redis(host=host, port=port, db=db)
        if flush:
            await ORedis.connection.flushdb()
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

    schema = (TextField("_id"),)
    field_names = vars(instance)
    for fieldname, value in field_names.items():
        if fieldname == "created_at" or fieldname == "updated_at" or fieldname == "_id":
            continue
        if fieldname in ["payload", "id", "$ne"]:
            raise Exception(f"TODO Error: cannot use {fieldname} as a field name at the moment")
        if issubclass(type(value), bool):
            schema = schema + (TextField(fieldname),)
        elif issubclass(type(value), int):
            schema = schema + (NumericField(fieldname),)
        elif issubclass(type(value), str):
            schema = schema + (TextField(fieldname),)
        else:
            schema = schema + (TextField(fieldname),)
    schema = schema + (NumericField("created_at"),)
    schema = schema + (NumericField("updated_at"),)

    try:
        cls.sync.ft(cls.index_name).create_index(schema, definition=definition)
        waitForIndex(cls.sync, cls.index_name)
    except Exception as e:
        # traceback_info = traceback.format_exc()
        # print(" +", traceback_info)
        # print(e)
        print("[ORedis] Info: Index exists already")

    original_init = cls.__init__

    def new(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        for fieldname, default_val in field_names.items():
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

        setattr(self, "_id", str(uuid_hex()))
        setattr(self, "created_at", get_current_timestamp())
        setattr(self, "updated_at", get_current_timestamp())

    cls.__init__ = new

    def toString(self):
        return json.dumps(self.__dict__, indent=4)

    cls.__str__ = toString
    cls.__repr__ = toString

    def create(doc_dict):
        inst = cls()
        for fieldname, default_val in field_names.items():
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

        setattr(inst, "_id", str(uuid_hex()))
        setattr(inst, "created_at", get_current_timestamp())
        setattr(inst, "updated_at", get_current_timestamp())
        
        return inst
    
    cls.create = create
    
    def _resolve_value_by_fieldname(field, value):
        final_val = value
        if issubclass(type(field_names[field]), bool):
            final_val = f'"{str(value)}"'
        elif issubclass(type(field_names[field]), float) or issubclass(type(field_names[field]), int):
            if type(value) == dict:
                if "$lt" in value or "$gt" in value:
                    lt = value["$lt"] if "$lt" in value else "+inf"
                    gt = value["$gt"] if "$gt" in value else "-inf"
                    final_val = f"[{str(gt)} {str(lt)}]"
                else: 
                    raise Exception(f"Error: Invalid value given to field: {field}, {str(value)}")
            else: 
                final_val = f"[{str(value)} {str(value)}]"
        elif issubclass(type(field_names[field]), str):
            if type(value) == list:
            #     all_strings = all(type(val) == str for val in value) # checking if all vals are strings
                possible_vals = filter(lambda val: type(val) == str, value)
                possible_vals = list(map(lambda val: f'"{val}"', possible_vals))
                possible_vals = "|".join(value)
                final_val = f"({possible_vals})"
            else:
                final_val = f'"{value}"'
        else:
            if type(value) == list:
            #     all_strings = all(type(val) == str for val in value) # checking if all vals are strings
                possible_vals = filter(lambda val: type(val) == str, value)
                possible_vals = list(map(lambda val: f'"{val}"', possible_vals))
                possible_vals = "|".join(value)
                final_val = f"({possible_vals})"
            else:
                final_val = f'"{value}"'

        return final_val
    
    async def termsAgg(fieldname: str):
        # print(field_names.keys())
        # print("============================================")
        # print(fieldname)
        # print("field in fieldNames", fieldname in field_names)
        if fieldname in field_names:
            req = aggregations.AggregateRequest("*").group_by(
                f"@{fieldname}", reducers.count()
            )

            res = await ORedis.connection.ft(cls.index_name).aggregate(req)
            results = []

            for row in res.rows:
                if bytes(fieldname, 'utf-8') not in row:
                    # print(bytes(fieldname, 'utf-8'))
                    # print("=========> NOT IN ROW")
                    # print(row)
                    continue
                i = row.index(bytes(fieldname, 'utf-8'))
                # print(i)
                # print(len(row))
                if i + 1 < len(row):
                    # print("good there is a next !!!!")
                    value = row[i+1]
                    cast_val = value
                    if issubclass(type(field_names[fieldname]), bool): 
                        cast_val = resolve_bool(value)
                    elif issubclass(type(field_names[fieldname]), float):
                        cast_val = float(value) 
                    elif issubclass(type(field_names[fieldname]), int):
                        cast_val = int(value)
                    elif issubclass(type(field_names[fieldname]), bytes):
                        cast_val =  str(value, 'utf-8')
                    else:
                        cast_val = str(value)

                    if b"__generated_aliascount" not in row:
                        raise Exception()
                    ii = row.index(b"__generated_aliascount")
                    doc_count = 0
                    if ii + 1 < len(row):
                        doc_count = int(row[ii+1])
                    else:
                        raise Exception()
                    
                    results.append({
                        f"{fieldname}": cast_val,
                        "doc_count": doc_count
                    })

            return results
        
        return []

    cls.termsAgg = termsAgg        

    def find(query) -> OQuery:
        q_arr = []
        sep = " "

        for field, value in query.items():
            if field in field_names:
                ne_prefix = ""
                if type(value) == dict:
                    if "$ne" in value:
                        ne_prefix = "-"
                        value = value["$ne"]

                final_val = _resolve_value_by_fieldname(field, value)
                q_arr.append(f"{ne_prefix}@{field}:{final_val}")

        q_str = sep.join(q_arr) if len(q_arr) else "*"
        print(q_str)
        oquery = OQuery(Query(q_str), cls)

        return oquery
        # res = cls.connection.ft(cls.index_name).search(Query(q_str).paging(start, end).sort_by(field="num_id", asc=False))
        # arr = []
        # for doc in res.docs:
        #     arr.append(cls.create(doc.__dict__))
        
        # return arr 

    cls.find = find

    async def findOne(query, sort_by="created_at", asc=False) -> cls:
        q_arr = []
        for field, value in query.items():
            if field in field_names:
                ne_prefix = ""
                if type(value) == dict:
                    if "$ne" in value:
                        ne_prefix = "-"
                        value = value["$ne"]

                if issubclass(type(field_names[field]), bool):
                    q_arr.append(f"{ne_prefix}@{field}:{str(value)}")
                elif issubclass(type(field_names[field]), float):
                    q_arr.append(f"{ne_prefix}@{field}:[{str(value)} {str(value)}]")
                elif issubclass(type(field_names[field]), int):
                    q_arr.append(f"{ne_prefix}@{field}:[{str(value)} {str(value)}]")
                elif issubclass(type(field_names[field]), str):
                    if type(value) == list:
                    #     all_strings = all(type(val) == str for val in value) # checking if all vals are strings
                        possible_vals = filter(lambda val: type(val) == str, value)
                        possible_vals = list(map(lambda val: f'"{val}"', possible_vals))
                        possible_vals = "|".join(value)
                        q_arr.append(f"{ne_prefix}@{field}:({possible_vals})")
                    else:
                        q_arr.append(f"{ne_prefix}@{field}:\"{value}\"")
                else:
                    if type(value) == list:
                    #     all_strings = all(type(val) == str for val in value) # checking if all vals are strings
                        possible_vals = filter(lambda val: type(val) == str, value)
                        possible_vals = list(map(lambda val: f'"{val}"', possible_vals))
                        possible_vals = "|".join(value)
                        q_arr.append(f"{ne_prefix}@{field}:({possible_vals})")
                    else:
                        q_arr.append(f"{ne_prefix}@{field}:\"{value}\"")
        q_str = " ".join(q_arr) if bool(q_arr) else "*"
        if sort_by is not None:
            res = await cls.connection.ft(cls.index_name).search(Query(q_str).sort_by(sort_by, asc=bool(asc)).paging(0, 1))
        else:
            res = await cls.connection.ft(cls.index_name).search(Query(q_str).paging(0, 1))

        one = None
        for doc in res.docs:
            one = cls.create(doc.__dict__)
        
        return one
    
    cls.findOne: cls = findOne

    async def insert(bulk):
        # pipe: Connection = cls.connection.pipeline()
        async with cls.connection.pipeline() as pipe:
            pipe_tmp = pipe
            for doc in bulk:
                try:
                    doc['_id'] = str(uuid_hex())
                    for field in doc:
                        val = doc[field]
                        cast_val = val
                        if issubclass(type(doc[field]), bool):
                            cast_val = str(val)
                        else:
                            pass 
                        doc[field] = cast_val
                    doc['created_at'] = get_current_timestamp()
                    doc['updated_at'] = get_current_timestamp()
                    pipe_tmp = pipe_tmp.hset(f"{cls.prefix}{doc['_id']}", mapping=doc)
                    # pipe.hset(f"{cls.prefix}{doc['_id']}", mapping=doc)
                except Exception as e:
                    traceback_info = traceback.format_exc()
                    print(" +", traceback_info)
                    print(e)

            oks = await pipe_tmp.execute()

    cls.insert = insert

    async def updateWhere(values, query):
        # pipe: Connection = cls.connection.pipeline()
        async with cls.connection.pipeline() as pipe:
            pipe_tmp = pipe
            q_arr = []
            for field, value in query.items():
                if field in field_names:
                    ne_prefix = ""
                    if type(value) == dict:
                        if "$ne" in value:
                            ne_prefix = "-"
                            value = value["$ne"]
                        else: 
                            raise Exception(f"Error: Invalid value given to field: {field}, {str(value)}")

                    if issubclass(type(field_names[field]), bool):
                        q_arr.append(f"{ne_prefix}@{field}:{str(value)}")
                    elif issubclass(type(field_names[field]), float):
                        q_arr.append(f"{ne_prefix}@{field}:[{str(value)} {str(value)}]")
                    elif issubclass(type(field_names[field]), int):
                        q_arr.append(f"{ne_prefix}@{field}:[{str(value)} {str(value)}]")
                    elif issubclass(type(field_names[field]), str):
                        if type(value) == list:
                        #     all_strings = all(type(val) == str for val in value) # checking if all vals are strings
                            possible_vals = filter(lambda val: type(val) == str, value)
                            possible_vals = list(map(lambda val: f'"{val}"', possible_vals))
                            possible_vals = "|".join(value)
                            q_arr.append(f"{ne_prefix}@{field}:({possible_vals})")
                        else:
                            q_arr.append(f"{ne_prefix}@{field}:\"{value}\"")
                    else:
                        if type(value) == list:
                        #     all_strings = all(type(val) == str for val in value) # checking if all vals are strings
                            possible_vals = filter(lambda val: type(val) == str, value)
                            possible_vals = list(map(lambda val: f'"{val}"', possible_vals))
                            possible_vals = "|".join(value)
                            q_arr.append(f"{ne_prefix}@{field}:({possible_vals})")
                        else:
                            q_arr.append(f"{ne_prefix}@{field}:\"{value}\"")
            q_str = " ".join(q_arr) if len(q_arr) else "*"
            res = await cls.connection.ft(cls.index_name).search(Query(q_str).paging(0, 10_000))
            arr = []
            for doc in res.docs:
                doc = doc.__dict__
                del doc["id"]
                del doc["payload"]
                for field, value in values.items():
                    if field in doc:
                        doc[field] = value
                doc['updated_at'] = get_current_timestamp()
                pipe_tmp = pipe_tmp.hset(f"{cls.prefix}{doc['_id']}", mapping=doc)
                arr.append(cls.create(doc))

            oks = await pipe_tmp.execute()

        return arr
    
    cls.updateWhere = updateWhere

    async def deleteAll():
        await cls.connection.ft(cls.index_name).dropindex(delete_documents=True)
        cls.sync.ft(cls.index_name).create_index(schema, definition=definition)
        waitForIndex(cls.sync, cls.index_name)

    cls.deleteAll = deleteAll

    async def save(self):
        self_dict = self.__dict__
        for field in self_dict:
            val = self_dict[field]
            cast_val = val
            if issubclass(type(self_dict[field]), bool):
                cast_val = str(val)
            else:
                pass
            self_dict[field] = cast_val
        self_dict['updated_at'] = get_current_timestamp()
        ok = await cls.connection.hset(f"{cls.prefix}{self._id}", mapping=self_dict)

        return self

    cls.save = save
    
    return cls

class OQueryInterface:
    def sortBy(self, field, asc=True):
        pass
        
    def limit(self, start=0, size=10_000):
        pass

    def exec(self, asDicts=False):
        pass

class Schema(ORedis):
    # Schema.__init__ has to be defined in Schema because it has to replace ORedis.__init__
    def __init__(self):
        # self._id = str(uuid_hex())
        pass

    def save(self):
        pass

    @classmethod
    def create(cls, doc_dict):
        pass

    @classmethod
    def find(cls, query, start=0, end=10_000) -> OQueryInterface:
        pass

    @classmethod
    def findOne(cls, query, sort_by, asc):
        pass

    @classmethod
    def insert(cls, bulk):
        pass

    @classmethod
    def updateWhere(cls, values, query):
        pass

    @classmethod
    def deleteAll(cls):
        pass

    @classmethod
    def termsAgg(cls, fieldname: str):
        pass

class OQuery(OQueryInterface):
    
    def __init__(self, search_query: Query, cls: Schema):
        self.search_query: Query = search_query.paging(offset=0, num=10_000)
        self.schema: Schema = cls

    def sortBy(self, field, asc=True) -> OQueryInterface:
        self.search_query.sort_by(field=field, asc=asc)
        return self
        
    def limit(self, start=0, size=10_000) -> OQueryInterface:
        self.search_query.paging(offset=start, num=size)
        return self

    async def exec(self, asDicts=False):
        res = await self.schema.connection.ft(self.schema.index_name).search(self.search_query)
        arr = []
        if not asDicts:
            for doc in res.docs:
                arr.append(self.schema.create(doc.__dict__))
        else:
            for doc in res.docs:
                arr.append(doc.__dict__)

        return arr