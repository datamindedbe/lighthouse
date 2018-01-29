package be.dataminded.lighthouse.common

import com.amazonaws.services.s3.model.S3Object
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}

import scala.io.Source

/**
  * Object helps abstract common file system operations
  */
object FileSystem {
  def read(path: String): String = {
    if (path.startsWith("s3"))
      new S3FileSystem().read(path)
    else
      new LocalFileSystem().read(path)
  }
}

trait FileSystem {
  def read(path: String): String
}

class S3FileSystem extends FileSystem {

  override def read(path: String): String = {
    val s3Client           = AmazonS3ClientBuilder.standard().build()
    val uri: AmazonS3URI   = new AmazonS3URI(path)
    val s3Object: S3Object = s3Client.getObject(uri.getBucket, uri.getKey)
    s3Object.getObjectContent

    val source = Source.fromInputStream(s3Object.getObjectContent)
    val lines = try source.mkString
    finally source.close()
    lines
  }
}

class LocalFileSystem extends FileSystem {
  override def read(path: String): String = {
    val source = scala.io.Source.fromFile(path)
    val lines = try source.mkString
    finally source.close()
    lines
  }
}
