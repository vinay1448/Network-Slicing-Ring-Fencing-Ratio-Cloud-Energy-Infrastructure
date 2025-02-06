package cloudsimresearchprojectfinal;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

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

public class PreprocessedCSV {

    private static List<Vm> vmList;
    private static List<Cloudlet> cloudletList;

    public static void main(String[] args) {

        // Step 1: Initialize CloudSim
        System.out.println("Starting CloudSim simulation...");

        PrintStream originalOut = System.out;  // Save original System.out
        PrintStream originalErr = System.err;  // Save original System.err

        try {
            // Redirect output to a file
            PrintStream out = new PrintStream(new FileOutputStream("C:\\Users\\vinay\\Cloudsim_ResearchProject\\simulation_output_full.txt"));
            System.setOut(out);  // Redirect System.out to the file
            System.setErr(out);  // Redirect System.err to the file

            // Redirect CloudSim's logging output to the same file
            Log.setOutput(out);

            // Initialize CloudSim for one iteration
            long iterationStart = System.nanoTime();  // Start time for the iteration

            // Initialize CloudSim library
            int num_user = 1;  // Number of users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;

            CloudSim.init(num_user, calendar, trace_flag);

            // Step 2: Create Datacenters (representing each network slice)
            Datacenter datacenter0 = createDatacenter("Datacenter_0", 1);  // Slice 1
            Datacenter datacenter1 = createDatacenter("Datacenter_1", 2);  // Slice 2
            Datacenter datacenter2 = createDatacenter("Datacenter_2", 3);  // Slice 3

            Log.printLine(datacenter0.getName() + " created.");
            Log.printLine(datacenter1.getName() + " created.");
            Log.printLine(datacenter2.getName() + " created.");

            // Step 3: Create Broker
            DatacenterBroker broker = createBroker();
            int brokerId = broker.getId();

            Log.printLine("Broker created with ID: " + brokerId);

            // Step 4: Create VMs
            vmList = createVMs(brokerId);  // Total VMs across all slices
            broker.submitVmList(vmList);

            Log.printLine("VMs submitted to the broker.");

            // Step 5: Load Cloudlets from preprocessed CSV files
            cloudletList = loadCloudletsFromCSV(brokerId, "C:/Users/vinay/Research Project/GWA-T-12BitbrainsPreprocessing/preprocessed_fastStorage.csv");
            cloudletList.addAll(loadCloudletsFromCSV(brokerId, "C:/Users/vinay/Research Project/GWA-T-12BitbrainsPreprocessing/preprocessed_rnd.csv"));

            // Reduce the number of cloudlets to simulate a lighter workload
            if (cloudletList.size() > 50) {
                cloudletList = cloudletList.subList(0, 50);  // Use only the first 50 cloudlets
            }

            broker.submitCloudletList(cloudletList);

            Log.printLine("Cloudlets submitted to the broker.");

            // Step 6: Simulate VM-to-VM communication
            simulateVMCommunication();

            // Step 7: Start the simulation
            CloudSim.startSimulation();
            Log.printLine("Simulation started.");

            // Step 8: Retrieve results when simulation ends
            List<Cloudlet> newList = broker.getCloudletReceivedList();
            CloudSim.stopSimulation();
            Log.printLine("Simulation stopped. Cloudlet processing results collected.");
            
            // Calculate Overall Task Completion Time
            double overallTaskCompletionTime = calculateOverallTaskCompletionTime(newList);
            
            // Log Response Time
            Log.printLine("=== System Metrics ===");
            Log.printLine("Overall Task Completion Time: " + overallTaskCompletionTime + " seconds");

            // Ensure this is called only once after the simulation has stopped
            double totalEnergy = calculateTotalEnergy(newList);
            double coolingPower = calculateCoolingPower(totalEnergy);
            double totalEnergyWithCooling = totalEnergy + coolingPower;

            // Log the outputs using Log.printLine and ensure it's written to the file by flushing
            Log.printLine("=== Energy Consumption Results ===");
            Log.printLine("Total Energy Consumption: " + totalEnergy + " Watts");
            Log.printLine("Cooling Power Consumption: " + coolingPower + " Watts");
            Log.printLine("Total Energy Consumption with Cooling: " + totalEnergyWithCooling + " Watts");

            long iterationEnd = System.nanoTime();  // End time for the iteration
            long iterationTime = iterationEnd - iterationStart;

            Log.printLine("Iteration completed in " + (iterationTime / 1_000_000) + " ms.");
            Log.printLine("Total convergence time: " + (iterationTime / 1_000_000) + " ms.");

            // Write energy consumption results to the output file
            writeEnergyResultsToFile("C:\\Users\\vinay\\Cloudsim_ResearchProject\\simulation_output_full.txt", totalEnergy, coolingPower, totalEnergyWithCooling, overallTaskCompletionTime);

            // Print Cloudlet results
            printCloudletList(newList);

            // Ensure the output is flushed at the end of the simulation
            System.out.flush();

            // Write energy consumption results to the output file
            writeEnergyResultsToFile("C:\\Users\\vinay\\Cloudsim_ResearchProject\\simulation_output_full.txt", totalEnergy, coolingPower, totalEnergyWithCooling, overallTaskCompletionTime);

            // Print Cloudlet results
            printCloudletList(newList);

            // Write Cloudlet results to a file
            writeCloudletResultsToFile(newList, "C:\\Users\\vinay\\Cloudsim_ResearchProject\\simulation_output_full.txt");

            Log.printLine("CloudSim simulation finished!");

            // Ensure the output is flushed at the end of the simulation
            System.out.flush();
            System.err.flush();

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Simulation failed due to an unexpected error.");
            System.out.flush();  // Ensure the error is also flushed to the file
        } finally {
            // Restore original System.out and System.err
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
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

    // Create Broker
    private static DatacenterBroker createBroker() {
        try {
            return new DatacenterBroker("Broker");
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // Create VMs (representing L3 switches in your ring fences)
    private static List<Vm> createVMs(int brokerId) {
        List<Vm> vmlist = new ArrayList<>();

        long size = 1000; // Image size (MB)
        int ram = 64; // VM memory (MB)
        int mips = 5;  // Reduced MIPS value to simulate lower processing power
        long bw = 100;
        int pesNumber = 1; // Number of CPUs
        String vmm = "Xen"; // VMM name

        // Network Slice 1 - 1 ring fence with 3 L3 switches
        for (int i = 0; i < 3; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Network Slice 2 - 2 ring fences, 2 L3 switches in each ring fence (4 total)
        for (int i = 3; i < 7; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        // Network Slice 3 - 3 ring fences with 1, 2, and 3 L3 switches respectively
        for (int i = 7; i < 13; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        return vmlist;
    }

    // Load and create Cloudlets from the preprocessed CSV files
    private static List<Cloudlet> loadCloudletsFromCSV(int brokerId, String csvFilePath) {
        List<Cloudlet> cloudletList = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            boolean firstLine = true;

            while ((line = br.readLine()) != null) {
                // Skip the header row
                if (firstLine) {
                    firstLine = false;
                    continue;
                }

                // Split by comma and trim spaces
                String[] values = line.split(",");
                for (int i = 0; i < values.length; i++) {
                    values[i] = values[i].trim();  // Trim whitespace around values
                }

                // Ensure that there are enough columns (at least 11 columns after trimming)
                if (values.length < 11) {
                    System.err.println("Invalid row with insufficient columns: " + line);
                    continue;
                }

                try {
                    // Parse the relevant values
                    int cores = parseInteger(values[1], 1);  // CPU_cores, default to 1 if parsing fails or is zero
                    double cpuUsageMHz = parseDouble(values[2], 5.0);  // Reduced CPU_usage_MHz

                    long fileSize = 100;  // Arbitrary file size value
                    long outputSize = 100;  // Arbitrary output size value

                    // Create Cloudlet object
                    Cloudlet cloudlet = new Cloudlet(
                        cloudletList.size(), (long) cpuUsageMHz, cores, fileSize, outputSize,
                        new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull()
                    );
                    cloudlet.setUserId(brokerId);
                    cloudletList.add(cloudlet);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid number format in row: " + line);
                    continue;  // Skip invalid rows
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return cloudletList;  // Return the list of cloudlets
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

    // Simulate VM-to-VM communication based on the architecture
    private static void simulateVMCommunication() {
    	 // Network Slice 1
        simulateCommunicationBetweenVMs(0, 1, 1000); // Ring Fence 1
        simulateCommunicationBetweenVMs(1, 0, 1000);

        // Network Slice 2
        // Ring Fence 1
        simulateCommunicationBetweenVMs(3, 5, 1500); // Inter-ring
        simulateCommunicationBetweenVMs(3, 8, 2000); // Inter-slice
        simulateCommunicationBetweenVMs(4, 8, 2000); // Inter-slice
        simulateCommunicationBetweenVMs(4, 12, 2500); // Inter-slice
        
        // Ring Fence 2
        simulateCommunicationBetweenVMs(5, 6, 1500); // Intra-ring
        simulateCommunicationBetweenVMs(5, 3, 1500); // Inter-ring
        simulateCommunicationBetweenVMs(5, 12, 2500); // Inter-slice

        // Network Slice 3
        // Ring Fence 1
        simulateCommunicationBetweenVMs(7, 11, 1500); // Inter-ring

        // Ring Fence 2
        simulateCommunicationBetweenVMs(8, 1, 2000); // Inter-slice
        simulateCommunicationBetweenVMs(8, 3, 2000); // Inter-slice
        simulateCommunicationBetweenVMs(8, 4, 2000); // Inter-slice

        // Ring Fence 3
        simulateCommunicationBetweenVMs(11, 7, 1500); // Inter-ring
        simulateCommunicationBetweenVMs(12, 4, 2500); // Inter-slice
        simulateCommunicationBetweenVMs(12, 5, 2500); // Inter-slice
    }

    // Helper method to simulate communication between two VMs
    private static void simulateCommunicationBetweenVMs(int vm1, int vm2, double bandwidth) {
        Log.printLine("Simulating communication between VM " + vm1 + " and VM " + vm2 + " with bandwidth: " + bandwidth);
        // Placeholder for communication simulation logic (can add delays or resource sharing based on bandwidth)
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

 // Function to write energy consumption results and task completion time to a file
    private static void writeEnergyResultsToFile(String outputFileName, double totalEnergy, double coolingPower, double totalEnergyWithCooling, double overallTaskCompletionTime) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFileName, true))) {
            writer.println("========== SYSTEM METRICS ==========");
            writer.println("Overall Task Completion Time: " + overallTaskCompletionTime + " seconds");

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
