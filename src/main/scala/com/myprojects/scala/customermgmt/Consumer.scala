package com.myprojects.scala.customermgmt

import java.util.ArrayList

object Consumer  {
	import java.util.Properties
	import java.util
	import java.util.Map
	import org.apache.kafka.clients.consumer.KafkaConsumer
	import org.apache.kafka.clients.consumer.ConsumerRecord
	import com.google.gson.Gson
	import java.sql.DriverManager
	import java.sql.Connection
	import com.myprojects.scala.customermgmt._
	
	def main(args: Array[String]): Unit = readFromKafkaConsumer

	def readFromKafkaConsumer() = {
		  val topic = "customer_topic"
      val  props = new Properties()
			props.put("bootstrap.servers", "localhost:9092")
			props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
			props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
			props.put("group.id", "something")

			val consumer = new KafkaConsumer[String, String](props)
      consumer.subscribe(util.Collections.singletonList(topic))

			while(true){
			  consumer.poll(1000).forEach{cr => processCustomerDetail(cr.value())}
			}
	}

	def processCustomerDetail(customerDetails: String) = {
			println("New message in the topic => "+customerDetails)
			try {
			  var customer = convertToCustomerObject(customerDetails)
			  validate(customer)
			  writeToDb(customer)
			} catch { case e: Exception => println(e.getMessage, e) }
	}

	def validate(customer: Customer) = {
	    if(customer.age > 99)
	      throw new ValidationException("Validation error occured. Age entered in invalid")
	    if(customer.firstName.length() > 50)
	      throw new ValidationException("Validation error occured. First Name cannot be greater than 50")
	    if(customer.lastName.length() > 50)
	      throw new ValidationException("Validation error occured. Last Name cannot be greater than 50")
	    if(customer.address.postalCode.length() > 5)
	      throw new ValidationException("Validation error occured. Invalid Zip Code")
	    if(customer.address.streetAddress.length() > 100)
	      throw new ValidationException("Validation error occured. Street Address cannot be greater than 100")
	    if(customer.address.city.length() > 25)
	      throw new ValidationException("Validation error occured. City cannot be greater than 25")
	    if(customer.address.state.length() > 2)
	      throw new ValidationException("Validation error occured. Please Enter 2 letter state code")
	    if(!(customer.phoneNumber.get(0).pType == "fax" || customer.phoneNumber.get(0).pType == "home" || customer.phoneNumber.get(1).pType == "fax" || customer.phoneNumber.get(1).pType == "home"))
	      throw new ValidationException("Validation error occured. Invalid Phone Number Type")
	}

	def convertToCustomerObject(customerDetails: String): Customer = {
	    try {
			  val gson = new Gson
			  gson.fromJson(customerDetails, classOf[Customer])
	    } catch { case e: Exception => throw new ValidationException("Exception in converting to Customer Oblect. Please check for valid Json format.") }
	}
		
	def writeToDb(customer: Customer) = {
			val driver = "org.sqlite.JDBC"
			val url = "jdbc:sqlite:/Users/sobinpaul/Myproject/eclipse-workspace/CustomerManagemet/CustMgmtConsumer/kafkaconsumer/customer_mgmt.db"
			var connection:Connection = null

			try {
				Class.forName(driver)
				connection = DriverManager.getConnection(url)
				val statement = connection.createStatement()
				val result = statement.executeUpdate("insert into CUSTOMER VALUES ('"+customer.firstName+"','"+customer.lastName+"',"+customer.age+",'"+customer.address.streetAddress+"', '"+customer.address.city+"','"+customer.address.state+"',"+customer.address.postalCode+",'"+customer.phoneNumber.get(0).number+"','"+customer.phoneNumber.get(1).number+"')")
			} catch { case e: Exception => throw new SystemException("Exception in inserting data into CUSTOMER table", e) }
			connection.close()
  }
case class PhoneNumber (pType: String, number: String)
case class Address (streetAddress: String, city: String, state: String, postalCode: String)
case class Customer (firstName: String, lastName: String, age: Int, address: Address, phoneNumber: ArrayList[PhoneNumber])

class ValidationException(message:String) extends Exception(message)
class SystemException(message: String, e: Exception) extends Exception(message)
}

