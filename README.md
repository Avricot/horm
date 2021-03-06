horm
====

Horm is a scal-hbase orm that map scala classes to hbase data.

Hbase data are easy to read (it uses field name to store datas), so the data is easy to read in a M/R batch without building the object with the orm.

Data is stored in the "data" column family, typicaly : 

    data.firstname => "quentin"
    data.id => 1
    data.myMap.myKey1 => "value of myKey1"
    data.mySubObject.field1 => "field1Value"
    data.myComplexMap.k1 => "myKey1 complex"
    data.myComplexMap.v1 => "myKey1 complex value"
    data.myList.0 => "List object 1"
    data.myList.1 => "List object 2"
    ...

###Horm initialization
#####Zookeeper configuration
Init the connection :

`HormConfig.init("localhost", 2181)`

#####Init the class table.
A table is created per class. If you want Horm to check if the table exists at the startup (and create it if not) :

`HormConfig.initTable(classOf[MyClass])`

###Class configuration
Your class needs to extends the trait HormBaseObject and define the method getHBaseId, as following :

    case class MyClass(id: Long, firstname: String) extends HormBaseObject {
       override def getHBaseId() = Bytes.toBytes(id)
    }

Add a companion object that extends HormObject to perform CRUD operation on your instanceS.

`object MyClass extends HormObject[MyClass]`
###Save an instance

    val instance = MyClass(1L, "quentin")
    MyClass.save(instance)


###Read an instance
`MyClass.find(Bytes.toBytes(1L))`

###delete an instance

    MyClass.delete(Bytes.toBytes(1L))
    //or
    MyClass.delete(instance)


###Supported type
Supported types are the following :


* classOf[Int]
* classOf[Long]
* classOf[Float]
* classOf[String]
* classOf[Boolean]
* classOf[Array[Byte]]
* classOf[org.joda.time.DateTime]
* classOf[scala.collection.Map[_, _]]
* classOf[scala.collection.mutable.Map[_, _]]
* classOf[scala.collection.immutable.Map[_, _]]
* classOf[scala.collection.mutable.WeakHashMap[_, _]] 
* classOf[scala.collection.mutable.OpenHashMap[_, _]] 
* classOf[scala.collection.mutable.LinkedHashMap[_, _]] 
* classOf[scala.collection.mutable.ListMap[_, _]] 
* classOf[scala.collection.mutable.HashMap[_, _]] 
* classOf[scala.collection.mutable.HashMap[_, _]] 
* classOf[scala.collection.immutable.HashMap[_, _]] 
* classOf[scala.collection.immutable.ListMap[_, _]] 
* classOf[scala.collection.Set[_]]
* classOf[scala.collection.mutable.Set[_]]
* classOf[scala.collection.immutable.Set[_]]
* classOf[scala.collection.Seq[_]]
* classOf[scala.collection.mutable.Seq[_]]
* classOf[scala.collection.immutable.Seq[_]]

* Any kind of objects composed of the previous types

###Map type
By default, maps are read/written as maps of [String, String]
If your map is a different kind, you need to add the HormMap annotation in order to build the map using reflection (as generic type are erased at compilation)

So for example :

    case class MyClass(id: Long, firstname: String, @(HormMap @field )(key=classOf[Boolean], value=classOf[Long]) myMap: Map[Boolean, Long]) extends HormBaseObject {
       override def getHBaseId() = Bytes.toBytes(id)
    }

Currently only primitive types are supported in map, not complex objects.

###List type
Same as Map, but with @HormList

    @(HormList @field )(klass=classOf[Long])

###Adding a raw binder
If you need to add a primitive conversion (an object is mapped as a unique value, for example int/String/boolean etc. or to store a Date), you need to add a Binder to the RawBinder.binders map :


    /**
     * Jodatime date binder
     */
    object DateTimeBinder extends RawBinder[DateTime] {
      RawBinder.binders(classOf[DateTime]) = this

      def read(param: Array[Byte]) = {
        new DateTime(Bytes.toLong(param))
      }
      def write(obj: Any): Array[Byte] = {
        Bytes.toBytes(obj.asInstanceOf[DateTime].getMillis())
     }
    }

###Adding a complex binder
If you need to add a complex conversion (for example to store an object with multiple fields), you need to add a Binder to the ComplexBinder.binders map :


    object ObjectBinder extends ComplexBinder[MyObject] {
      ComplexBinder.binders(classOf[MyObject]) = this

      override def read(objArgs: Map[String, Map[String, Array[Byte]]], klass: Class[_], family: String, currentField: Field) = {
          //...build the object from the sb
      }

      override def write(family: Array[Byte], fieldName: String, field: Field, obj: Any, put: Put) = {
         // Store the object to the db.
      }

    }


###Region number scan
Horm provide method to scan values in a given range, but splitted with the region number as first byte separator (See HormObject documentation)

This is usefull to retrieve sorted data with timestamp as key, but stored with a random first byte :

    01353402735
    01353402736
    21353402737
    21353402738
    31353402739


