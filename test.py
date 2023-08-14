from core import ORedis

om = ORedis(host='localhost', port=6381, db=0, flush=True)
# print("test connection =============================:", om.connection)

from Person import Person

# print("Person connection ===================:", Person.connection)
bulk = [
    {"person_id": 1, "name": 'foo', "email": "foo@bar.com", "age": 10, "employed": True},
    {"person_id": 2, "name": "bar", "email": "bar@baz.com", "age": 20, "employed": False},
    {"person_id": 3, "name": 'baz', "email": "baz@buzz.com", "age": 30, "employed": True},
    {"person_id": 4, "name": 'foo', "email": "foo@bar.com", "age": 10, "employed": False}
]

print("hello world")

Person.insert(bulk)

# person1: Person = Person.create({"person_id": 1, "name": 'foo', "email": "foo@bar.com", "age": 10, "employed": True})
# person1 = person1.save()

# person2 = Person(person_id=2, name="bar", email="bar@baz.com", age=20, employed=False)
# person2 = person2.save()

# person3: Person = Person.create({"person_id": 3, "name": 'baz', "email": "baz@buzz.com", "age": 30, "employed": True})
# person3 = person3.save()

# person4: Person = Person.create({"person_id": 4, "name": 'foo', "email": "foo@bar.com", "age": 10, "employed": False})
# person4 = person4.save()

people: Person = Person.find({})
print("find query:")
print(people)