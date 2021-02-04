package be.dataminded.lighthouse.paramstore

import be.dataminded.lighthouse.datalake.LazyConfig
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest

/**
  * Class to help retrieve parameters from the AWS parameter store using [[GetParameterRequest]]
  */
class AwsSsmParamStore private () extends ParameterStore {

  private lazy val client = AWSSimpleSystemsManagementClientBuilder.defaultClient()

  /**
    * Returns the parameter value coming from the AWS SSM parameter store as an anonymous function. Allowing to defer
    * the retrieval only when absolutely necessary
    *
    * @param ssmPath The AWS SSM path where to retrieve the parameter value from
    * @return An anonymous function allowing to retrieve the configuration path
    */
  def lookup(ssmPath: String): LazyConfig[String] =
    LazyConfig {
      val getParameterRequest = new GetParameterRequest().withName(ssmPath).withWithDecryption(true)
      client.getParameter(getParameterRequest).getParameter.getValue
    }
}

/**
  * Default SSM param store object to avoid duplicate creation
  */
object AwsSsmParamStore extends AwsSsmParamStore
