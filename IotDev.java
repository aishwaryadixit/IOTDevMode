/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.datatorrent.stram.api;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedHashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.input.ClassLoaderObjectInputStream;

import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.StringCodecs;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Created by User on 1/6/17.
 */
public class IotDev implements Serializable
{
  static String name = Connect.getUsername().trim();
  public static final String SER_FILE_NAME = "/home/" + name + "/file";
  private static final Logger LOG = LoggerFactory.getLogger(IotDev.class);
  public  static LogicalPlan lp;

  public IotDev()
  {
  }

  public static LinkedHashSet<URL> read(InputStream is) throws IOException, ClassNotFoundException
  {
    return (LinkedHashSet<URL>)new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), is).readObject();
  }

  public static URLClassLoader loadDependencies(LinkedHashSet<URL> launchDependencies)
  {
    URLClassLoader cl = URLClassLoader.newInstance(launchDependencies.toArray(new URL[launchDependencies.size()]));
    Thread.currentThread().setContextClassLoader(cl);
    StringCodecs.check();
    return cl;
  }


  public static void main(String[]args)throws IOException, InterruptedException, ClassNotFoundException
  {
    ClassLoader loader = IotDev.class.getClassLoader();
    LOG.debug(String.valueOf(loader.getResource("\n\nPATH : \nfoo/Test.class")));



    FileInputStream f = new FileInputStream("/home/" + name + "/fileURL");
    LinkedHashSet<URL> launchDependencies = read(f);
    loadDependencies(launchDependencies);

    LOG.debug("\n\nURLS\n\n");

    for (URL l : launchDependencies) {
      LOG.debug("\n URL " + l);
    }

    // FileInputStream fis = new FileInputStream("./" + LogicalPlan.SER_FILE_NAME);
    FileInputStream fis = new FileInputStream("/home/" + name + "/file");
    try {
      lp = LogicalPlan.read(fis);
    } finally {
      fis.close();
    }
    StramLocalCluster lc = new StramLocalCluster(lp);
    lc.run();
  }
}

