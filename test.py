from core import ORedis

om = ORedis(host='localhost', port=6381, db=0)

from Person import Person


bulk = [
    {"person_id": 1, "name": 'foo', "email": "foo@bar.com", "age": 10},
    {"person_id": 2, "name": "bar", "email": "bar@baz.com", "age": 20},
    {"person_id": 3, "name": 'baz', "email": "baz@buzz.com", "age": 30},
    {"person_id": 4, "name": 'foo', "email": "foo@bar.com", "age": 10}
]

# person1: Person = Person.create({"person_id": 1, "name": 'foo', "email": "foo@bar.com", "age": 10})
# person1 = person1.save()

# person2 = Person(person_id=2, name="bar", email="bar@baz.com", age=20)
# person2 = person2.save()

# person3: Person = Person.create({"person_id": 3, "name": 'baz', "email": "baz@buzz.com", "age": 30})
# person3 = person3.save()

# person4: Person = Person.create({"person_id": 4, "name": 'foo', "email": "foo@bar.com", "age": 10})
# person4 = person4.save()

Person.insert(bulk)

people: Person = Person.find({})
print(people)