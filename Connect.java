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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.StringTokenizer;

import javax.ws.rs.QueryParam;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.stram.StreamingContainerManager;
import com.datatorrent.stram.codec.LogicalPlanSerializer;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.util.JSONSerializationProvider;

/**
 * Created by aishwarya on 2/6/17.
 */
public class Connect implements Serializable
{
  private static final Logger LOG = LoggerFactory.getLogger(IotDev.class);

  public static LogicalPlan getLp()
  {
    return lp;
  }

  public static void setLp(LogicalPlan lp)
  {
    Connect.lp = lp;
  }

  public static LogicalPlan lp;

  public static HashSet<String> apps = new HashSet<>();

  public static boolean cleanup = true;

  public static HashMap<String,String> process = new HashMap<>();

  private String status;
  private static ObjectMapper objectMapper = new JSONSerializationProvider().getContext(null);
  private static StreamingContainerManager dagManager;
  private static String username = getUsername();


  public Connect()
  {

  }


  public Connect(LogicalPlan lp) throws IOException, InterruptedException, ClassNotFoundException, JSONException
  {
    cleanup = false;
    LOG.debug("\n\nSTARTING CONNECT " + cleanup + " \n\n");
    setLp(lp);
    create();
  }

  public void create() throws IOException, InterruptedException, ClassNotFoundException, JSONException
  {
    //Write logical plan to a file

    LOG.debug("\n USERNAME " + username);
    username = username.trim();
    FileOutputStream fs = new FileOutputStream("/home/" + username + "/file");
    LogicalPlan.write(lp,fs);
    //Fork new JVM for the launched application
    String separator = System.getProperty("file.separator");
    String classpath = System.getProperty("java.class.path");
    String path = System.getProperty("java.home")
        + separator + "bin" + separator + "java";
    ProcessBuilder processBuilder = new ProcessBuilder(path, "-cp", classpath, IotDev.class.getName(), "&");
    Map<String, String> env = processBuilder.environment();
    env.put("CLASSPATH", "/home/" + username + "/apex/apex-malhar-master/library/target");
    Thread.sleep(2000);
    final Process process = processBuilder.start();
    try {
      if (!isAlive(process)) {
        int ev = process.exitValue();
        LOG.debug("\n\nExitValue " + ev);
      } else {
        LOG.debug("NOt exited");
      }
    } catch (Exception e) {
      LOG.debug("Process is still running");
    }

    Long pid = getPidOfProcess(process);
    IotModeCommands iot = new IotModeCommands();
    String startTime = getStartTime(pid);

    //create meta file for the applications launched in the IOT Dev Mode
    LOG.debug("\n\nUser " + getUsername());

    File f = new File("/home/" + username + "/MetaFile");
    if (!f.exists()) {
      LOG.debug("Creating a new file ");
      FileWriter fw = new FileWriter(new File("/home/" + username + "/MetaFile"));
      fw.close();
    }
    FileReader fr = new FileReader(f);
    BufferedReader br = new BufferedReader(fr);
    long i = 0;


    if (br.readLine() == null) {
      br.close();
      fr.close();
      LOG.debug("Starting the first application");
      LOG.debug("\n\n Writing \n\n");
      FileWriter fw = new FileWriter(f);
      BufferedWriter bw = new BufferedWriter(fw);
      fw.write("{App ID: " + (i + 1) + "}" );
      bw.write("\n{Status:  starting app}\n ");
      bw.write("{Start Time: " + startTime + "}\n");
      if (!apps.contains("App ID " + (i + 1))) {
        apps.add("App ID " + (i + 1));
      }

      changeStatus(i + 1);
      bw.close();
      fw.close();
      br.close();
      fr.close();

    } else {
      i = 0;
      File ff = new File("/home/" + username + "/MetaFile");
      FileReader frr = new FileReader(ff);
      BufferedReader brr = new BufferedReader(frr);

      String aid = null;
      if (ff.exists()) {
        String s;
        while ((s = brr.readLine()) != null) {
          if (s.length() == 0) {
            continue;
          }
          if (s.contains("App ID")) {
            LOG.debug("\nindex " + s.length());
            aid = s.substring(8, s.length() - 1);
            aid = aid.trim();
            i = Long.parseLong(aid);
            if (!apps.contains("App ID " + aid)) {
              apps.add("App ID " + aid);
            }
            fr.close();
            br.close();
          }
        }
        f = new File("/home/" + username + "/MetaFile");
        FileWriter fw = new FileWriter(f, true);
        BufferedWriter bw = new BufferedWriter(fw);
        if (!apps.contains("App ID " + (i + 1))) {
          apps.add("App ID " + ( i + 1));
        }

        String meta = "\n{App ID: " + (i + 1) + "}\n{Status:  starting app}\n{Start Time: " + startTime + "}\n";
        JSONObject jsonObject = new JSONObject(meta);
        bw.write(meta);
        bw.close();
        fw.close();
      }
    }

    LOG.debug(String.valueOf(apps));


    LOG.debug("\nProcess ID " + pid);
    changeStatus(i + 1);
    saveAppMeta(pid,(i + 1));
    writeLogicalPlanMeta(lp,(i + 1));

    //Cleaning Buffers
    final StringBuilder sb = new StringBuilder();
    final Thread outThread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
          // Read stream here
          sb.append(reader);


        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    });


    final Thread errThread = new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
        // Read stream here
          sb.append(reader);
          LOG.debug(reader.readLine());
        } catch (Exception e) {
          LOG.debug(e.getStackTrace().toString());
        }
      }
    });

    outThread.start();
    errThread.start();

    new Thread(new Runnable()
    {
      @Override
      public void run()
      {
        int exitCode = -1;
        try {
          exitCode = process.waitFor();
          outThread.join();
          errThread.join();
        } catch (Exception e) {
          LOG.debug(e.getStackTrace().toString());
        }

        // Process completed and read all stdout and stderr here
      }
    }).start();
//



  }

// to return the running status of the application
  public static boolean isAlive(Process p)
  {
    try {
      p.exitValue();
      return false;
    } catch (Exception e) {
      return true;
    }
  }

  //to write the meta file per application
  public static void saveAppMeta(long pid,long appid) throws IOException, InterruptedException
  {
    //check if directory IotApps exists else create
    IotModeCommands iot = new IotModeCommands();
    LOG.debug("\n\nString pid: " + String.valueOf(pid));
    String status = getProcessStatus(String.valueOf(pid));
    LOG.debug("\n\n Creating a new File " + appid + "\n\n");
    File file = new File("/home/" + username + "/IotApps/AppMeta" + appid);
    if (!file.exists()) {
      boolean f = new File("/home/" + username + "/IotApps").mkdir();
    }
    FileWriter fw = new FileWriter(file);
    BufferedWriter bw = new BufferedWriter(fw);
    String meta = "{APP ID: " + appid + "}\n{Process ID: " + pid + "} \n{Process Status:\n" + status + "}";
    bw.write(meta);
    bw.close();
    fw.close();
  }

  //to append the logical plan into the meta file created for each application
  public static void writeLogicalPlanMeta(LogicalPlan lp, long appid) throws IOException, JSONException
  {

    FileWriter fw = new FileWriter(new File("/home/" + username + "/IotApps/AppMeta" + appid), true);
    BufferedWriter bw = new BufferedWriter(fw);
    JSONObject l = new JSONObject(objectMapper.writeValueAsString(LogicalPlanSerializer.convertToMap(
        lp, true)));
    bw.write("\n{Logical PLan: \n" +  String.valueOf(l) + "}");
    bw.close();
    fw.close();

  }

  public JSONObject getLogicalPlan(@QueryParam("includeModules") String includeModules) throws JSONException, IOException
  {
    return new JSONObject(objectMapper.writeValueAsString(LogicalPlanSerializer.convertToMap(
        dagManager.getLogicalPlan(), includeModules != null)));
  }

  private static String output(InputStream inputStream) throws IOException
  {
    StringBuilder sb = new StringBuilder();
    BufferedReader br = null;
    try {
      br = new BufferedReader(new InputStreamReader(inputStream));
      String line = null;
      while ((line = br.readLine()) != null) {
        sb.append(line + System.getProperty("line.separator"));
      }
    } finally {
      br.close();
    }
    return sb.toString();
  }

  //to get the process id of each application
  public static synchronized long getPidOfProcess(Process p)
  {
    long pid = -1;

    try {
      if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
        Field f = p.getClass().getDeclaredField("pid");
        f.setAccessible(true);
        pid = f.getLong(p);
        f.setAccessible(false);
      }
    } catch (Exception e) {
      pid = -1;
    }
    return pid;
  }

  public void changeStatus(long appid) throws IOException
  {
    BufferedReader br = new BufferedReader(new FileReader(new File("/home/" + username + "/MetaFile")));
    String s = "";
    String line = "";

    while ((s = br.readLine()) != null) {
      if (s.length() == 0) {
        continue;
      }
      LOG.debug("appid is " + appid);
      if (s.equals("{App ID: " + appid + "}")) {
        line = line + s;
        s = br.readLine();

        LOG.debug("checking s " + s );
       // if (s.equals("Status:  starting app")) {
        s = "\n{Status:  running app}";
       // }

      }
      line = line + s + "\n";

      LOG.debug("Line is "  +  line);
    }
    br.close();
    FileWriter fw = new FileWriter(new File("/home/" + username + "/MetaFile"));
    BufferedWriter bw = new BufferedWriter(fw);
    bw.write(line);
    bw.close();
    fw.close();
  }

  public static String getUsername()
  {
    IotModeCommands iot  = new IotModeCommands();

    String command = "whoami";
    String user = executeCommand(command);
    return user;
  }

  public static void getProcesses()
  {
    String command = "jps";
    IotModeCommands iot = new IotModeCommands();
    String output = executeCommand(command);
    StringTokenizer str = new StringTokenizer(output);
    output = executeCommand(command);


    while (str.hasMoreTokens()) {
      String pid = str.nextToken();
      String pname = str.nextToken();

      if (pname.equals("IotDev")) {
        process.put(pid, pname);
      }
    }
  }

  public static String getStartTime(long pid) throws IOException
  {
    String start = null;
    Runtime runtime = Runtime.getRuntime();
    Process process = runtime.exec("ps -ewo pid,lstart");
    BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    String line = "";
    while ((line = reader.readLine()) != null) {
      line = line.trim();
      if (line.startsWith(pid + " ")) {
        int firstSpace = line.indexOf(" ");
        start = line.substring(firstSpace + 1);
        break;
      }
    }
    return start;
  }


  public static String executeCommand(String command)
  {
    StringBuffer output = new StringBuffer();

    Process p;
    try {
      p = Runtime.getRuntime().exec(command);
      p.waitFor();
      BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
      String line = "";
      while ((line = reader.readLine()) != null) {
        output.append(line + "\n");
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return output.toString();

  }

  public static String getProcessStatus(String pid) throws IOException, InterruptedException
  {
    getProcesses();
    String output = "";
    for (Map.Entry<String, String> g : process.entrySet()) {
      String p = g.getKey();
      if (p.equals(pid)) {
        String command = "ps -p " + p;
        output = executeCommand(command);

      }
    }
    return output;
  }
}




