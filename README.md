horm
====

Horm is a scal-hbase orm that map scala classes to hbase data.
Hbase data are easy to read (it uses field name to store datas), so the data is easy to read in a M/R batch without building the object with the orm.
Data is stored in the "data" column family, typicaly : 

    data.firstname => "quentin"
    data.id => 1
    data.id => 1
    data.myMap.k1 => "myKey1"
    data.myMap.v1 => "value of myKey1"
    data.mySubObject.field1 => "field1Value"
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
* classOf[org.joda.time.DateTime]
* classOf[Array[Byte]]
* classOf[scala.collection.mutable.Map[_, _]]
* classOf[scala.collection.immutable.Map[_, _]]
* Any kind of objects composed of the previous types

###Map type
By default, maps are read/written as maps of [String, String]
If your map is a different kind, you need to add the HormMap annotation in order to build the map using reflection (as generic type are erased at compilation)

So for example :

    case class MyClass(id: Long, firstname: String, @(HormMap @field )(key=classOf[Boolean], value=classOf[Long]) myMap: Map[Boolean, Long]) extends HormBaseObject {
       override def getHBaseId() = Bytes.toBytes(id)
    }

Currently only primitive types are supported in map, not complex objects.

###Adding a raw binder
If you need to add a primitive conversion (for example to store a Date), you need to add a Binder to the Binder.binders map :


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

