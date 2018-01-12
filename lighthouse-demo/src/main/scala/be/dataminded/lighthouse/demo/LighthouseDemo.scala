package be.dataminded.lighthouse.demo

import be.dataminded.lighthouse.datalake._
import be.dataminded.lighthouse.paramstore.AwsSsmParamStore
import be.dataminded.lighthouse.spark.SparkApplication
import org.apache.spark.sql.{Dataset, SaveMode}

// DemoWarehouse or Warehouse could be provided via external classpath as well
object DemoDatalake extends Datalake {
  val ssmConfig: AwsSsmParamStore = new AwsSsmParamStore

  environment("prod") { references =>
    references += (Volvo.UID -> new FileSystemDataLink("s3://bucket/raw/volvo"))
    references += (Audi.UID  -> new HiveDataLink("s3://bucket/base/audi", "cars", "audi"))
    references += (Mercedes.UID -> new JdbcDataLink(
      ssmConfig.lookup("/ssm/url/path"),
      ssmConfig.lookup("/ssm/user/path"),
      ssmConfig.lookup("/ssm/pw/path"),
      "com.mysql.jdbc.Driver",
      "mercedes",
      saveMode = SaveMode.Append
    ))
  }

  environment("test") { references =>
    references += (Volvo.UID    -> new FileSystemDataLink("data/raw/volvo"))
    references += (Audi.UID     -> new FileSystemDataLink("data/base/audi"))
    references += (Mercedes.UID -> new FileSystemDataLink("data/base/mercedes"))
  }
}

case class Audi(id: Int, carType: String, price: Float)
object Audi {
  val UID: DataUID = DataUID("raw.cars", "audi")
}

case class Volvo(id: Int, carType: String, price: Float)
object Volvo {
  val UID: DataUID = DataUID("base.cars", "volvo")
}

case class Mercedes(id: Int, carType: String, price: Float)
object Mercedes {
  val UID: DataUID = DataUID("export.cars", "mercedes")
}

object LighthouseDemo extends SparkApplication {

  import spark.implicits._

  // Get audi data ref
  // TODO: properly document the system parameter that will be used for your environment
  // TODO: Hide Volvo.metadata away with implicits? This also allows the type to be known in the datareference class as well
  val volvoDataRef: DataLink = DemoDatalake.getDataLink(Volvo.UID)
  // convert audi to volvo
  val volvoData: Dataset[Volvo] = volvoDataRef.readAs[Volvo]()
  // Turn Volvo into Audi
  val audiData: Dataset[Audi] = volvoData.map[Audi]((volvo: Volvo) => Audi(volvo.id, volvo.carType, volvo.price))
  // get VolvoDataRef
  // TODO: Hide Audi.metadata away with implicits? This also allows the type to be known in the datareference class as well
  val audiDataRef: DataLink = DemoDatalake.getDataLink(Audi.UID)
  // write volvo data
  audiDataRef.write(audiData)
}
