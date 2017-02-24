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

import java.util.concurrent.TimeUnit

import com.google.common.util.concurrent.SettableFuture
import io.fabric8.kubernetes.api.model.Service
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import scala.collection.JavaConverters._

import org.apache.spark.deploy.kubernetes.KubernetesResourceCleaner
import org.apache.spark.deploy.kubernetes.constants._
import org.apache.spark.internal.Logging

private[spark] class ExternalUriSetWatcher(externalUriFuture: SettableFuture[String])
  extends Watcher[Service] with Logging {

  override def eventReceived(action: Action, service: Service): Unit = {
    if (action == Action.MODIFIED && !externalUriFuture.isDone) {
      service
        .getMetadata
        .getAnnotations
        .asScala
        .get(ANNOTATION_RESOLVED_EXTERNAL_URI)
        .foreach(externalUriFuture.set)
    }
  }

  override def onClose(cause: KubernetesClientException): Unit = {
    logDebug("External URI set watcher closed.", cause)
  }
}

private[spark] class ExternalSuppliedUrisDriverServiceManager(
    kubernetesClient: KubernetesClient,
    kubernetesResourceCleaner: KubernetesResourceCleaner,
    kubernetesAppId: String,
    driverSelectors: Map[String, String],
    uriProvisionTimeoutSeconds: Long)
  extends DriverServiceManager with Logging {

  private val externalUriFuture = SettableFuture.create[String]
  private var externalUriSetWatch: Option[Watch] = None

  override def getDriverServiceSubmissionServerUris(driverService: Service): Set[String] = {
    require(externalUriSetWatch.isDefined, "The watch that listens for the provision of" +
      " the external URI was not started; was createDriverService called?")
    Set(externalUriFuture.get(uriProvisionTimeoutSeconds, TimeUnit.SECONDS))
  }

  override def createDriverService: Service = {
    externalUriSetWatch = Some(kubernetesClient
      .services()
      .withName(kubernetesAppId)
      .watch(new ExternalUriSetWatcher(externalUriFuture)))
    val service = kubernetesClient.services().create(
      createDefaultServiceModel(kubernetesAppId, driverSelectors)
        .editMetadata()
          .addToAnnotations(ANNOTATION_PROVIDE_EXTERNAL_URI, "true")
          .endMetadata()
        .editSpec()
          .withType("ClusterIP")
          .endSpec()
        .build())
    kubernetesResourceCleaner.registerOrUpdateResource(service)
    service
  }

  override def close(): Unit = {
    externalUriSetWatch.foreach(_.close())
  }
}

