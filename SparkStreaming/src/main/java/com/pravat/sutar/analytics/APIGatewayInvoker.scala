package pack;
import java.io._
import org.apache.commons._
import org.apache.http._
import org.apache.http.client._
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SparkSession, DataFrame }

object APIGatewayInvoker {

  def sendSFCOutput(data: DataFrame): String = {

    println("Data received at API Gateway Invoker:")
    data.writeStream.format("console").outputMode("append").start() 

    val post = new HttpPost("http://ip.jsontest.com/")
    val nameValuePairs = new ArrayList[NameValuePair]()
    nameValuePairs.add(new BasicNameValuePair("JSON", data.toString()))
    post.setEntity(new UrlEncodedFormEntity(nameValuePairs))

    println("Data to send")
    val client = new DefaultHttpClient
    val response = client.execute(post)
 
    response.getAllHeaders.foreach(arg => println(arg))
    println("Status Code:" + response.getStatusLine.getStatusCode)
    return response.getStatusLine.toString()
  }

}

