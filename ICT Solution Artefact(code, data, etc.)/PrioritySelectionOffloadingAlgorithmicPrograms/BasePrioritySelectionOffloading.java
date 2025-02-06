package cloudsimresearchprojectfinal;

import java.io.*;
import java.util.*;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

public class BasePrioritySelectionOffloading {

    private static List<Vm> vmList;
    private static List<Cloudlet> cloudletList;

    // Variable to hold the total energy consumption
    private static double totalEnergy = 0.0;

    public static void main(String[] args) {

        try {
            // Redirect output to a file
            String outputFilePath = "C:\\Users\\vinay\\Cloudsim_ResearchProject\\simulation_output_basepriorityselectionoffloadingalgorithm.txt";
            PrintStream out = new PrintStream(new FileOutputStream(outputFilePath));
            System.setOut(out);  // Output to file
            
            // Initialize CloudSim library
            int num_user = 1;  // Number of users
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;
            CloudSim.init(num_user, calendar, trace_flag);

            // Step 2: Create Datacenters (representing each network slice)
            Datacenter datacenter0 = createDatacenter("Datacenter_0", 1);
            Datacenter datacenter1 = createDatacenter("Datacenter_1", 2);
            Datacenter datacenter2 = createDatacenter("Datacenter_2", 3);
            
            // Logging creation of datacenters
            out.println(datacenter0.getName() + " created.");
            out.println(datacenter1.getName() + " created.");
            out.println(datacenter2.getName() + " created.");

            // Step 3: Create Broker
            DatacenterBroker broker = createBroker();
            int brokerId = broker.getId();
            out.println("Broker created with ID: " + brokerId);

            // Step 4: Create VMs
            vmList = createVMs(brokerId);
            broker.submitVmList(vmList);
            out.println("VMs created and submitted.");

            // Step 5: Load Cloudlets from preprocessed CSV files
            cloudletList = loadCloudletsFromCSV(brokerId, "C:/Users/vinay/Research Project/GWA-T-12BitbrainsPreprocessing/preprocessed_fastStorage.csv");
            cloudletList.addAll(loadCloudletsFromCSV(brokerId, "C:/Users/vinay/Research Project/GWA-T-12BitbrainsPreprocessing/preprocessed_rnd.csv"));
            broker.submitCloudletList(cloudletList);
            out.println("Cloudlets loaded and submitted.");

            // Step 6: Simulate VM-to-VM communication and offloading
            simulatePrioritySelectionOffloading(broker);
            simulateVMCommunication();  // Simulate the VM-to-VM communication

            // Step 7: Start the simulation
            CloudSim.startSimulation();
            List<Cloudlet> newList = broker.getCloudletReceivedList();
            CloudSim.stopSimulation();
            out.println("Simulation completed.");

            // Step 8: Calculate total energy and cooling power after the simulation ends
            double coolingPower = calculateCoolingPower(totalEnergy);
            double totalEnergyWithCooling = totalEnergy + coolingPower;

            // Write the total energy consumption results to the file via System.out
            out.println("========== ENERGY CONSUMPTION RESULTS ==========");
            out.println("Total Energy Consumption: " + totalEnergy + " Watts");
            out.println("Cooling Power Consumption: " + coolingPower + " Watts");
            out.println("Total Energy Consumption with Cooling: " + totalEnergyWithCooling + " Watts");
            
            // Log Cloudlet execution results
            printCloudletList(newList, out);
            writeCloudletResultsToFile(newList, outputFilePath);  // Ensure cloudlet results are written to the file

            // Close the output stream properly
            out.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Simulate VM-to-VM communication based on uneven network slicing ring fencing architecture
    private static void simulateVMCommunication() {
        simulateCommunicationBetweenVMs(0, 1, 1); // Lower bandwidth
        simulateCommunicationBetweenVMs(1, 0, 1);
        simulateCommunicationBetweenVMs(3, 5, 3);
        simulateCommunicationBetweenVMs(3, 8, 3);
        simulateCommunicationBetweenVMs(4, 8, 3);
        simulateCommunicationBetweenVMs(4, 12, 3);
        simulateCommunicationBetweenVMs(5, 6, 3);
        simulateCommunicationBetweenVMs(5, 3, 3);
        simulateCommunicationBetweenVMs(5, 12, 3);
        simulateCommunicationBetweenVMs(7, 11, 3);
        simulateCommunicationBetweenVMs(8, 1, 3);
        simulateCommunicationBetweenVMs(8, 3, 3);
        simulateCommunicationBetweenVMs(8, 4, 3);
        simulateCommunicationBetweenVMs(11, 7, 3);
        simulateCommunicationBetweenVMs(12, 4, 3);
        simulateCommunicationBetweenVMs(12, 5, 3);
    }

    private static void simulateCommunicationBetweenVMs(int vm1, int vm2, double bandwidth) {
        // Suppressing output of VM-to-VM communication
    }

    // Priority Selection Offloading
    private static void simulatePrioritySelectionOffloading(DatacenterBroker broker) {
        for (Vm vm : vmList) {
            double transmissionPower = 5;  // Reduced transmission power
            double systemChannelGain = 50;  // Increased system channel gain
            for (Cloudlet cloudlet : cloudletList) {
                // Compute local execution energy
                double localExecutionTime = cloudlet.getCloudletLength() / vm.getMips();
                double localEnergyConsumption = localExecutionTime * 15;  // Further reduced local energy consumption

                // Compute transmission energy (offloading energy)
                double transmissionEnergy = transmissionPower * cloudlet.getCloudletLength() / systemChannelGain;

                // Compare local execution energy and offloading energy
                if (localEnergyConsumption > transmissionEnergy) {
                    totalEnergy += transmissionEnergy;  // Offloading energy
                    broker.submitCloudletList(Collections.singletonList(cloudlet));
                } else {
                    totalEnergy += localEnergyConsumption;  // Local execution energy
                }
            }
        }
    }

    // Create Datacenter (Each datacenter represents a network slice)
    private static Datacenter createDatacenter(String name, int numHosts) {
        List<Host> hostList = new ArrayList<>();
        for (int i = 0; i < numHosts; i++) {
            List<Pe> peList = new ArrayList<>();
            int mips = 500;  // Further reduced MIPS
            peList.add(new Pe(0, new PeProvisionerSimple(mips)));

            int ram = 512;  // Reduced RAM
            long storage = 200000;  // Reduced storage
            int bw = 2000;  // Reduced bandwidth

            hostList.add(new Host(i, new RamProvisionerSimple(ram), new BwProvisionerSimple(bw), storage, peList, new VmSchedulerTimeShared(peList)));
        }
        DatacenterCharacteristics characteristics = new DatacenterCharacteristics("x86", "Linux", "Xen", hostList, 3.0, 1.0, 0.01, 0.05, 0.05);  // Further reduced power characteristics
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

    // Create VMs
    private static List<Vm> createVMs(int brokerId) {
        List<Vm> vmlist = new ArrayList<>();
        long size = 4000;  // Further reduced image size
        int ram = 64;  // Further reduced VM memory
        int mips = 400;  // Further reduced MIPS value
        long bw = 100;  // Further reduced bandwidth
        int pesNumber = 1;
        String vmm = "Xen";

        for (int i = 0; i < 12; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }
        return vmlist;
    }

    // Load Cloudlets
    private static List<Cloudlet> loadCloudletsFromCSV(int brokerId, String csvFilePath) {
        List<Cloudlet> cloudletList = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            boolean firstLine = true;

            while ((line = br.readLine()) != null) {
                if (firstLine) {
                    firstLine = false;
                    continue;
                }
                String[] values = line.split(",");
                if (values.length < 11) {
                    continue;  // Ignore invalid rows
                }
                try {
                    int cores = parseInteger(values[1], 1);
                    double cpuUsageMHz = parseDouble(values[2], 600.0);  // Further reduced CPU usage MHz
                    long fileSize = 150;  // Further reduced file size
                    long outputSize = 150;  // Further reduced output size

                    Cloudlet cloudlet = new Cloudlet(cloudletList.size(), (long) cpuUsageMHz, cores, fileSize, outputSize,
                            new UtilizationModelFull(), new UtilizationModelFull(), new UtilizationModelFull());
                    cloudlet.setUserId(brokerId);
                    cloudletList.add(cloudlet);
                } catch (NumberFormatException e) {
                    // Suppressing invalid row output
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return cloudletList;
    }

    // Helper methods for parsing values
    private static int parseInteger(String value, int defaultValue) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static double parseDouble(String value, double defaultValue) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    // Calculate cooling power consumption
    public static double calculateCoolingPower(double totalEnergyConsumption) {
        double coolingFactor = 0.03;  // Further reduced cooling factor
        return totalEnergyConsumption * coolingFactor;
    }

    // Print Cloudlet execution results
    private static void printCloudletList(List<Cloudlet> list, PrintStream out) {
        String indent = "    ";
        out.println("========== OUTPUT ==========");
        out.println("Cloudlet ID" + indent + "STATUS" + indent + "Data center ID" + indent + "VM ID" + indent + "Time" + indent + "Start Time" + indent + "Finish Time");

        for (Cloudlet cloudlet : list) {
            out.print(indent + cloudlet.getCloudletId() + indent + indent);
            if (cloudlet.getStatus() == Cloudlet.SUCCESS) {
                out.print("SUCCESS");
                out.println(indent + cloudlet.getResourceId() + indent + cloudlet.getVmId() + indent + cloudlet.getActualCPUTime() + indent + cloudlet.getExecStartTime() + indent + cloudlet.getFinishTime());
            }
        }
    }

    // Write Cloudlet results to a file
    private static void writeCloudletResultsToFile(List<Cloudlet> list, String outputFileName) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFileName, true))) {
            String indent = "    ";
            writer.println("========== OUTPUT ==========");
            writer.println("Cloudlet ID" + indent + "STATUS" + indent + "Data center ID" + indent + "VM ID" + indent + "Time" + indent + "Start Time" + indent + "Finish Time");

            for (Cloudlet cloudlet : list) {
                writer.print(indent + cloudlet.getCloudletId() + indent + indent);
                if (cloudlet.getStatus() == Cloudlet.SUCCESS) {
                    writer.print("SUCCESS");
                    writer.println(indent + cloudlet.getResourceId() + indent + cloudlet.getVmId() + indent + cloudlet.getActualCPUTime() + indent + cloudlet.getExecStartTime() + indent + cloudlet.getFinishTime());
                }
            }
            writer.flush();
        } catch (IOException e) {
            System.err.println("Error writing results to file: " + e.getMessage());
        }
    }
}
