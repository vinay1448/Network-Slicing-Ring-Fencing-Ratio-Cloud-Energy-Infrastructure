package lambdapreprocessresearchprojectfinal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.cloudbus.cloudsim.Cloudlet;
import org.cloudbus.cloudsim.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import software.amazon.awssdk.services.cloudwatch.CloudWatchClient;

import software.amazon.awssdk.services.cloudwatch.model.PutMetricDataRequest;
import software.amazon.awssdk.services.cloudwatch.model.MetricDatum;
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit;
import java.time.Instant;

public class PreprocessedCSVScalableLambda implements RequestHandler<Map<String, String>, String> {

    private static List<Vm> vmList;
    private static List<Cloudlet> cloudletList;
    private static double totalEnergy = 0.0;
    
    @Override
    public String handleRequest(Map<String, String> event, Context context) {
        try {
            String bucketName = "aggregatedlambdabucketoutput";
            String inputFastStorageKey = "preprocessed_fastStorage.csv";
            String inputRndKey = "preprocessed_rnd.csv";
            String outputFileKey = "simulation_output_aggregated.txt" + UUID.randomUUID() + ".txt";

            // Initialize AWS S3 Client
            AmazonS3 s3Client = AmazonS3ClientBuilder.defaultClient();

            // Read input CSV files from S3
            InputStream fastStorageStream = s3Client.getObject(bucketName, inputFastStorageKey).getObjectContent();
            InputStream rndStream = s3Client.getObject(bucketName, inputRndKey).getObjectContent();

            // Process Simulation
            File outputFile = new File("/tmp/" + outputFileKey);
            processSimulation(fastStorageStream, rndStream, outputFile);

            // Upload output file back to S3
            s3Client.putObject(bucketName, outputFileKey, outputFile);

            return "Simulation completed successfully. Output uploaded to S3 bucket: " + bucketName;
        } catch (Exception e) {
            return "Simulation failed: " + e.getMessage();
        }
    }

    public static void processSimulation(InputStream fastStorageStream, InputStream rndStream, File outputFile) throws Exception {
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;

        try {
            PrintStream out = new PrintStream(new FileOutputStream(outputFile));
            System.setOut(out);
            System.setErr(out);
            Log.setOutput(out);

            long iterationStart = System.nanoTime();

            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;

            CloudSim.init(num_user, calendar, trace_flag);

            Datacenter datacenter0 = createDatacenter("Datacenter_0", 1);
            Datacenter datacenter1 = createDatacenter("Datacenter_1", 2);
            Datacenter datacenter2 = createDatacenter("Datacenter_2", 3);
            Datacenter datacenter3 = createDatacenter("Datacenter_3", 4);
            Datacenter datacenter4 = createDatacenter("Datacenter_4", 5);

            Log.printLine(datacenter0.getName() + " created.");
            Log.printLine(datacenter1.getName() + " created.");
            Log.printLine(datacenter2.getName() + " created.");
            Log.printLine(datacenter3.getName() + " created.");
            Log.printLine(datacenter4.getName() + " created.");

            DatacenterBroker broker = createBroker();
            int brokerId = broker.getId();
            Log.printLine("Broker created with ID: " + brokerId);

            vmList = createVMs(brokerId);
            broker.submitVmList(vmList);

            Log.printLine("VMs submitted to the broker.");

            cloudletList = loadCloudletsFromStream(brokerId, fastStorageStream);
            cloudletList.addAll(loadCloudletsFromStream(brokerId, rndStream));

            if (cloudletList.size() > 50) {
                cloudletList = cloudletList.subList(0, 50);
            }
            broker.submitCloudletList(cloudletList);
            Log.printLine("Cloudlets submitted to the broker.");

            simulateVMCommunication();
            simulateIdleVM(11);
            simulateIdleVM(21);
            simulateIdleVM(27);

            CloudSim.startSimulation();
            Log.printLine("Simulation started.");

            List<Cloudlet> newList = broker.getCloudletReceivedList();
            CloudSim.stopSimulation();
            Log.printLine("Simulation stopped. Cloudlet processing results collected.");

            // Call calculateTotalEnergy with cloudlets to include their energy consumption
            totalEnergy += calculateTotalEnergy(newList); // Aggregate energy from cloudlets

            // Calculate Overall Task Completion Time
            double overallTaskCompletionTime = calculateOverallTaskCompletionTime(newList);
            double coolingPower = calculateCoolingPower(totalEnergy);
            double totalEnergyWithCooling = totalEnergy + coolingPower;
            long iterationEnd = System.nanoTime(); // End time for the iteration
            long iterationTime = iterationEnd - iterationStart;
            
            // Publish Metrics
            publishCustomMetrics(overallTaskCompletionTime, totalEnergy, coolingPower, totalEnergyWithCooling);

            // Publish iteration time to CloudWatch
            logMetricsToCloudWatch("Iteration Time", iterationTime / 1_000_000);

            // Write iteration time to S3 bucket
            try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile, true))) {
                writer.println("Iteration Time: " + (iterationTime / 1_000_000) + " ms");
            }

            // Log other metrics to CloudWatch
            logMetricsToCloudWatch("Task Completion Time", overallTaskCompletionTime);
            logMetricsToCloudWatch("Total Energy Consumption", totalEnergy);
            logMetricsToCloudWatch("Cooling Power Consumption", coolingPower);
            logMetricsToCloudWatch("Total Energy With Cooling", totalEnergyWithCooling);

            // Log metrics to the console
            Log.printLine("=== Metrics ===");
            Log.printLine("Overall Task Completion Time: " + overallTaskCompletionTime + " seconds");
            Log.printLine("Total Energy Consumption: " + totalEnergy + " Watts");
            Log.printLine("Cooling Power Consumption: " + coolingPower + " Watts");
            Log.printLine("Total Energy Consumption with Cooling: " + totalEnergyWithCooling + " Watts");
            Log.printLine("Iteration completed in " + (iterationTime / 1_000_000) + " ms.");
            Log.printLine("Total convergence time: " + (iterationTime / 1_000_000) + " ms.");

            writeEnergyResultsToFile(outputFile.getAbsolutePath(), totalEnergy, coolingPower, totalEnergyWithCooling);
            // Print cloudlet results
            printCloudletList(newList);
            writeCloudletResultsToFile(newList, outputFile.getAbsolutePath());

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Simulation failed: " + e.getMessage());
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }
    
    // Modified method to load cloudlets from an InputStream instead of a local file
    private static List<Cloudlet> loadCloudletsFromStream(int brokerId, InputStream inputStream) {
        List<Cloudlet> cloudletList = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            boolean firstLine = true;

            while ((line = br.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue;
                }

                String[] values = line.split(",");
                for (int i = 0; i < values.length; i++) {
                    values[i] = values[i].trim();
                }

                if (values.length < 11) {
                    System.err.println("Invalid row with insufficient columns: " + line);
                    continue;
                }

                try {
                    int cores = parseInteger(values[1], 1);
                    double cpuUsageMHz = parseDouble(values[2], 5.0);

                    long fileSize = 100;
                    long outputSize = 100;

                    Cloudlet cloudlet = new Cloudlet(
                            cloudletList.size(), (long) cpuUsageMHz, cores, fileSize, outputSize,
                            new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull()
                    );
                    cloudlet.setUserId(brokerId);
                    cloudletList.add(cloudlet);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid number format in row: " + line);
                    continue;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return cloudletList;
    }

 // Create Datacenter (Each datacenter represents a network slice)
    private static Datacenter createDatacenter(String name, int numHosts) {
        List<Host> hostList = new ArrayList<>();

        // Create Hosts (Ring Fences)
        for (int i = 0; i < numHosts; i++) {
            List<Pe> peList = new ArrayList<>();
            int mips = 5;  // Further reduced MIPS to 5
            peList.add(new Pe(0, new PeProvisionerSimple(mips)));  // Processing Element (CPU core)

            int ram = 256; // Host memory (MB)
            long storage = 100000; // Host storage (MB)
            int bw = 1000; // Bandwidth

            hostList.add(new Host(
                    i,
                    new RamProvisionerSimple(ram),
                    new BwProvisionerSimple(bw),
                    storage,
                    peList,
                    new VmSchedulerTimeShared(peList)
            ));
        }

        String arch = "x86"; // Architecture
        String os = "Linux"; // Operating system
        String vmm = "Xen"; // Virtual machine monitor

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, 5.0, 2.0, 0.01, 0.01, 0.01);

        try {
            return new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), new LinkedList<>(), 0);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static DatacenterBroker createBroker() {
        try {
            return new DatacenterBroker("Broker");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static List<Vm> createVMs(int brokerId) {
        List<Vm> vmlist = new ArrayList<>();

        long size = 1000; // Image size (MB)
        int ram = 64; // VM memory (MB)
        int mips = 5;  // Reduced MIPS value to simulate lower processing power
        long bw = 100;
        int pesNumber = 1; // Number of CPUs
        String vmm = "Xen"; // VMM name

        // Network Slice 1 - 2 Ring Fences
        // Ring Fence 1: VMs 0, 1, 2
        for (int i = 0; i <= 2; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Ring Fence 2: VMs 3, 4
        for (int i = 3; i <= 4; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Network Slice 2 - 3 Ring Fences
        // Ring Fence 1: VMs 5, 6
        for (int i = 5; i <= 6; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Ring Fence 2: VMs 7, 8, 9
        for (int i = 7; i <= 9; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Ring Fence 3: VMs 10, 11 (VM 11 is isolated)
        for (int i = 10; i <= 11; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Network Slice 3 - 1 Ring Fence: VMs 12, 13, 14, 15
        for (int i = 12; i <= 15; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Network Slice 4 - 2 Ring Fences
        // Ring Fence 1: VMs 16, 17, 18
        for (int i = 16; i <= 18; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Ring Fence 2: VMs 19, 20, 21, 22 (VM 21 is isolated)
        for (int i = 19; i <= 22; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Network Slice 5 - 3 Ring Fences
        // Ring Fence 1: VMs 23, 24
        for (int i = 23; i <= 24; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Ring Fence 2: VMs 25, 26, 27 (VM 27 is isolated)
        for (int i = 25; i <= 27; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Ring Fence 3: VMs 28, 29
        for (int i = 28; i <= 29; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        return vmlist;
    }

    // Helper method to parse integers with default fallback
    private static int parseInteger(String value, int defaultValue) {
        try {
            int result = Integer.parseInt(value);
            return (result != 0) ? result : defaultValue;  // Ensure cores are non-zero
        } catch (NumberFormatException e) {
            return defaultValue;  // Return default value if parsing fails
        }
    }

    // Helper method to parse doubles with default fallback
    private static double parseDouble(String value, double defaultValue) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return defaultValue;  // Return default value if parsing fails
        }
    }

    // Calculate total energy consumption
    private static double calculateTotalEnergy(List<Cloudlet> cloudlets) {
        double totalEnergy = 0.0;
        double processingPower = 0.05;  // Further reduced processing power to 0.05 watts
        for (Cloudlet cloudlet : cloudlets) {
            totalEnergy += cloudlet.getActualCPUTime() * processingPower;
        }
        return totalEnergy;
    }

    // Further reduced cooling power consumption
    public static double calculateCoolingPower(double totalEnergyConsumption) {
        double coolingFactor = 0.0001;  // Reduced cooling overhead to 0.01%
        return totalEnergyConsumption * coolingFactor;
    }

 // Simulate VM-to-VM communication based on the aggregated uneven network slicing ring fencing architecture
    private static void simulateVMCommunication() {
        // Network Slice 1
        // Ring Fence 1
        simulateCommunicationBetweenVMs(0, 1, 1000);
        simulateCommunicationBetweenVMs(1, 0, 1000);
        simulateCommunicationBetweenVMs(1, 2, 1000);
        simulateCommunicationBetweenVMs(1, 4, 2000);  // inter-ring to Ring Fence 2 in Slice 1
        simulateCommunicationBetweenVMs(2, 1, 1000);

        // Ring Fence 2
        simulateCommunicationBetweenVMs(3, 4, 1500);
        simulateCommunicationBetweenVMs(3, 6, 2500);  // inter-slice to Slice 2
        simulateCommunicationBetweenVMs(4, 1, 2000);  // inter-ring connection to Ring Fence 1 in Slice 1
        simulateCommunicationBetweenVMs(4, 3, 1500);
        simulateCommunicationBetweenVMs(4, 8, 3000);  // inter-slice to Slice 2

        // Network Slice 2
        // Ring Fence 1
        simulateCommunicationBetweenVMs(5, 6, 1200);
        simulateCommunicationBetweenVMs(5, 22, 2800);  // inter-slice to Slice 4
        simulateCommunicationBetweenVMs(6, 3, 2500);   // inter-slice to Slice 1
        simulateCommunicationBetweenVMs(6, 5, 1200);

        // Ring Fence 2
        simulateCommunicationBetweenVMs(7, 8, 1400);
        simulateCommunicationBetweenVMs(8, 4, 3000);   // inter-slice to Slice 1
        simulateCommunicationBetweenVMs(8, 7, 1400);
        simulateCommunicationBetweenVMs(8, 9, 1400);
        simulateCommunicationBetweenVMs(8, 10, 2000);  // inter-ring to Ring Fence 3 in Slice 2
        simulateCommunicationBetweenVMs(9, 8, 1400);

        // Ring Fence 3
        simulateCommunicationBetweenVMs(10, 8, 2000);  // inter-ring to Ring Fence 2 in Slice 2
        simulateCommunicationBetweenVMs(10, 18, 2800); // inter-slice to Slice 4
        simulateCommunicationBetweenVMs(10, 24, 3200); // inter-slice to Slice 5

        // Network Slice 3
        // Ring Fence 1
        simulateCommunicationBetweenVMs(12, 13, 1300);
        simulateCommunicationBetweenVMs(13, 12, 1300);
        simulateCommunicationBetweenVMs(13, 14, 1300);
        simulateCommunicationBetweenVMs(14, 13, 1300);
        simulateCommunicationBetweenVMs(14, 15, 1300);
        simulateCommunicationBetweenVMs(15, 14, 1300);

        // Network Slice 4
        // Ring Fence 1
        simulateCommunicationBetweenVMs(16, 17, 1600);
        simulateCommunicationBetweenVMs(16, 23, 2700); // inter-slice to Slice 5
        simulateCommunicationBetweenVMs(17, 16, 1600);
        simulateCommunicationBetweenVMs(17, 18, 1600);
        simulateCommunicationBetweenVMs(17, 19, 1800); // inter-ring to Ring Fence 2 in Slice 4
        simulateCommunicationBetweenVMs(18, 10, 2800); // inter-slice to Slice 2
        simulateCommunicationBetweenVMs(18, 17, 1600);
        simulateCommunicationBetweenVMs(18, 22, 1800); // inter-ring to Ring Fence 2 in Slice 4

        // Ring Fence 2
        simulateCommunicationBetweenVMs(19, 17, 1800); // inter-ring to Ring Fence 1 in Slice 4
        simulateCommunicationBetweenVMs(19, 20, 1500);
        simulateCommunicationBetweenVMs(20, 19, 1500);
        simulateCommunicationBetweenVMs(22, 18, 1800); // inter-ring to Ring Fence 1 in Slice 4
        simulateCommunicationBetweenVMs(22, 5, 2800);  // inter-slice to Slice 2

        // Network Slice 5
        // Ring Fence 1
        simulateCommunicationBetweenVMs(23, 16, 2700); // inter-slice to Slice 4
        simulateCommunicationBetweenVMs(23, 24, 1600);
        simulateCommunicationBetweenVMs(24, 10, 3200); // inter-slice to Slice 2
        simulateCommunicationBetweenVMs(24, 23, 1600);
        simulateCommunicationBetweenVMs(24, 26, 2200); // inter-ring to Ring Fence 2 in Slice 5

        // Ring Fence 2
        simulateCommunicationBetweenVMs(25, 26, 1500);
        simulateCommunicationBetweenVMs(26, 24, 2200); // inter-ring to Ring Fence 1 in Slice 5
        simulateCommunicationBetweenVMs(26, 25, 1500);

        // Ring Fence 3
        simulateCommunicationBetweenVMs(28, 29, 1400);
        simulateCommunicationBetweenVMs(29, 28, 1400);

        // Include idle energy for isolated VMs (11, 21, 27)
        simulateIdleVM(11);
        simulateIdleVM(21);
        simulateIdleVM(27);
    }

 // Method to simulate idle energy consumption for isolated VMs
    private static void simulateIdleVM(int vmId) {
        double idleEnergyConsumption = 50; // Assume a small constant energy consumption for idle state
        totalEnergy += idleEnergyConsumption;
    }

    private static void simulateCommunicationBetweenVMs(int vm1, int vm2, double bandwidth) {
   	 Log.printLine("Simulating communication between VM " + vm1 + " and VM " + vm2 + " with bandwidth: " + bandwidth);

   	    // Calculate the energy consumption for the communication (as an example)
   	    double energyConsumption = bandwidth * 0.001;  // Replace with your formula
   	    totalEnergy += energyConsumption;  // Update the total energy
   }
    
    private static double calculateOverallTaskCompletionTime(List<Cloudlet> cloudlets) {
        double earliestStartTime = Double.MAX_VALUE;
        double latestFinishTime = Double.MIN_VALUE;

        for (Cloudlet cloudlet : cloudlets) {
            if (cloudlet.getExecStartTime() < earliestStartTime) {
                earliestStartTime = cloudlet.getExecStartTime();
            }
            if (cloudlet.getFinishTime() > latestFinishTime) {
                latestFinishTime = cloudlet.getFinishTime();
            }
        }

        return latestFinishTime - earliestStartTime;
    }
    
    private static void logMetricsToCloudWatch(String metricName, double value) {
        try (CloudWatchClient cloudWatchClient = CloudWatchClient.create()) {
            PutMetricDataRequest request = PutMetricDataRequest.builder()
                .namespace("AggregatedMetricsMonitor")
                .metricData(MetricDatum.builder()
                    .metricName(metricName)
                    .value(value)
                    .unit(StandardUnit.SECONDS) // Adjust unit as per metric
                    .timestamp(Instant.now()) // Use Instant for current timestamp
                    .build())
                .build();

            cloudWatchClient.putMetricData(request);
            Log.printLine("Logged metric " + metricName + " to CloudWatch with value: " + value);
        } catch (Exception e) {
            System.err.println("Error logging metrics to CloudWatch: " + e.getMessage());
        }
    }
    
    private static void publishCustomMetrics(double taskCompletionTime, double energyConsumption, double coolingPowerConsumption, double totalEnergyWithCooling) {
        try (CloudWatchClient cloudWatch = CloudWatchClient.create()) {
            String namespace = "AggregatedMetricsMonitor"; 

            List<MetricDatum> metrics = new ArrayList<>();

            // Add Task Completion Time
            metrics.add(MetricDatum.builder()
                    .metricName("TaskCompletionTime")
                    .unit(StandardUnit.MILLISECONDS) // Assuming taskCompletionTime is in milliseconds
                    .value(taskCompletionTime)
                    .build());

            // Add Energy Consumption
            metrics.add(MetricDatum.builder()
                    .metricName("EnergyConsumption")
                    .unit(StandardUnit.NONE) // None as per the custom metric setup
                    .value(energyConsumption)
                    .build());

            // Add Cooling Power Consumption
            metrics.add(MetricDatum.builder()
                    .metricName("CoolingPowerConsumption")
                    .unit(StandardUnit.NONE)
                    .value(coolingPowerConsumption)
                    .build());

            // Add Total Energy with Cooling
            metrics.add(MetricDatum.builder()
                    .metricName("TotalEnergyWithCooling")
                    .unit(StandardUnit.NONE)
                    .value(totalEnergyWithCooling)
                    .build());

            // Build the PutMetricDataRequest
            PutMetricDataRequest request = PutMetricDataRequest.builder()
                    .namespace(namespace)
                    .metricData(metrics)
                    .build();

            // Publish metrics to CloudWatch
            cloudWatch.putMetricData(request);

            System.out.println("Metrics published to CloudWatch.");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Failed to publish metrics to CloudWatch: " + e.getMessage());
        }
    }
    
    // Function to write energy consumption results to a file
    private static void writeEnergyResultsToFile(String outputFileName, double totalEnergy, double coolingPower, double totalEnergyWithCooling) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFileName, true))) {
            writer.println("========== ENERGY CONSUMPTION RESULTS ==========");
            writer.println("Total Energy Consumption: " + totalEnergy + " Watts");
            writer.println("Cooling Power Consumption: " + coolingPower + " Watts");
            writer.println("Total Energy Consumption with Cooling: " + totalEnergyWithCooling + " Watts");
            writer.flush();  // Ensure all data is written to the file
        } catch (IOException e) {
            System.err.println("Error writing energy results to file: " + e.getMessage());
        }
    }

    // Print Cloudlet execution results
    private static void printCloudletList(List<Cloudlet> list) {
        String indent = "    ";
        Log.printLine("========== OUTPUT ==========");
        Log.printLine("Cloudlet ID" + indent + "STATUS" + indent +
                "Data center ID" + indent + "VM ID" + indent + "Time" + indent + "Start Time" + indent + "Finish Time");

        for (Cloudlet cloudlet : list) {
            Log.print(indent + cloudlet.getCloudletId() + indent + indent);

            if (cloudlet.getStatus() == Cloudlet.SUCCESS) {
                Log.print("SUCCESS");

                Log.printLine(indent + cloudlet.getResourceId() + indent + cloudlet.getVmId() +
                        indent + cloudlet.getActualCPUTime() +
                        indent + cloudlet.getExecStartTime() + indent + cloudlet.getFinishTime());
            }
        }
    }

    // Function to write the Cloudlet results to a file
    private static void writeCloudletResultsToFile(List<Cloudlet> list, String outputFileName) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFileName, true))) {
            String indent = "    ";
            writer.println("========== OUTPUT ==========");
            writer.println("Cloudlet ID" + indent + "STATUS" + indent +
                    "Data center ID" + indent + "VM ID" + indent + "Time" + indent +
                    "Start Time" + indent + "Finish Time");

            for (Cloudlet cloudlet : list) {
                writer.print(indent + cloudlet.getCloudletId() + indent + indent);

                if (cloudlet.getStatus() == Cloudlet.SUCCESS) {
                    writer.print("SUCCESS");

                    writer.println(indent + cloudlet.getResourceId() + indent + cloudlet.getVmId() +
                            indent + cloudlet.getActualCPUTime() +
                            indent + cloudlet.getExecStartTime() + indent + cloudlet.getFinishTime());
                }
            }
            writer.flush();  // Ensure all data is written to the file
            System.out.println("Results written to: " + outputFileName);
        } catch (IOException e) {
            System.err.println("Error writing results to file: " + e.getMessage());
        }
    }
}