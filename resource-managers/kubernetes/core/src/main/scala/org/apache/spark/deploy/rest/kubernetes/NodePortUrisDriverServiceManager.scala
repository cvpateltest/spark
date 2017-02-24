/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.deploy.rest.kubernetes

import io.fabric8.kubernetes.api.model.Service
import io.fabric8.kubernetes.client.KubernetesClient
import scala.collection.JavaConverters._

import org.apache.spark.deploy.kubernetes.{KubernetesResourceCleaner, SslConfiguration}
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging

private[spark] class NodePortUrisDriverServiceManager(
    kubernetesClient: KubernetesClient,
    kubernetesSslConfiguration: SslConfiguration,
    kubernetesResourceCleaner: KubernetesResourceCleaner,
    kubernetesAppId: String,
    driverSelectors: Map[String, String])
  extends DriverServiceManager with Logging {

  override def getDriverServiceSubmissionServerUris(driverService: Service): Set[String] = {
    val urlScheme = if (kubernetesSslConfiguration.sslOptions.enabled) {
      "https"
    } else {
      logWarning("Submitting application details, application secret, and local" +
        " jars to the cluster over an insecure connection. You should configure SSL" +
        " to secure this step.")
      "http"
    }
    val servicePort = driverService.getSpec.getPorts.asScala
      .filter(_.getName == SUBMISSION_SERVER_PORT_NAME)
      .head.getNodePort
    val nodeUrls = kubernetesClient.nodes.list.getItems.asScala
      .filterNot(node => node.getSpec.getUnschedulable != null &&
        node.getSpec.getUnschedulable)
      .flatMap(_.getStatus.getAddresses.asScala)
      // The list contains hostnames, internal and external IP addresses.
      // (https://kubernetes.io/docs/admin/node/#addresses)
      // we want only external IP addresses and legacyHostIP addresses in our list
      // legacyHostIPs are deprecated and will be removed in the future.
      // (https://github.com/kubernetes/kubernetes/issues/9267)
      .filter(address => address.getType == "ExternalIP" || address.getType == "LegacyHostIP")
      .map(address => {
        s"$urlScheme://${address.getAddress}:$servicePort"
      }).toSet
    require(nodeUrls.nonEmpty, "No nodes found to contact the driver!")
    nodeUrls
  }

  override def createDriverService: Service = {
    val service = kubernetesClient.services().create(
      createDefaultServiceModel(kubernetesAppId, driverSelectors).editSpec()
        .withType("NodePort")
        .endSpec()
      .build())
    kubernetesResourceCleaner.registerOrUpdateResource(service)
    service
  }

  override def close(): Unit = {}
}
