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

class AggregatedSlice {
    int id;
    double minLatency;
    double maxLatency;
    List<Cloudlet> ueList;

    public AggregatedSlice(int id, double minLatency, double maxLatency) {
        this.id = id;
        this.minLatency = minLatency;
        this.maxLatency = maxLatency;
        this.ueList = new ArrayList<>();
    }
}

class AggregatedCluster {
    int id;
    List<Cloudlet> ueList;

    public AggregatedCluster(int id) {
        this.ueList = new ArrayList<>();
    }
}

public class AggregatedHeuristicAugment {

	 // Declare totalEnergy at the class level to accumulate energy costs
    private static double totalEnergy = 0.0;

    private static List<Vm> vmList;
    private static List<Cloudlet> cloudletList;

    public static void main(String[] args) {

        System.out.println("Starting CloudSim simulation...");

        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;

        try {
            PrintStream out = new PrintStream(new FileOutputStream("C:\\Users\\vinay\\Cloudsim_ResearchProject\\simulation_output_aggregatedheuristicaugmentupdatednewthree.txt"));
            System.setOut(out);
            System.setErr(out);

            Log.setOutput(out);

            // Start timing for the iteration and convergence
            long iterationStartTime = System.nanoTime();

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

            cloudletList = loadCloudletsFromCSV(brokerId, "C:/Users/vinay/Research Project/GWA-T-12BitbrainsPreprocessing/preprocessed_fastStorage.csv");
            cloudletList.addAll(loadCloudletsFromCSV(brokerId, "C:/Users/vinay/Research Project/GWA-T-12BitbrainsPreprocessing/preprocessed_rnd.csv"));
            broker.submitCloudletList(cloudletList);

            Log.printLine("Cloudlets submitted to the broker.");

            simulateVMCommunication();
            simulateIdleVM(11);
            simulateIdleVM(21);
            simulateIdleVM(27);

            // Start CloudSim simulation
            Log.printLine("Starting CloudSim simulation...");
            CloudSim.startSimulation();

            List<Cloudlet> newList = broker.getCloudletReceivedList();
            CloudSim.stopSimulation();
            Log.printLine("Simulation stopped. Cloudlet processing results collected.");
            
            double overallTaskCompletionTime = calculateOverallTaskCompletionTime(newList);

            Log.printLine("=== System Metrics ===");
            Log.printLine("Overall Task Completion Time: " + overallTaskCompletionTime + " seconds");

            // Calculate total energy consumption
            totalEnergy = calculateTotalEnergy(newList);
            double coolingPower = calculateCoolingPower(totalEnergy);
            double totalEnergyWithCooling = totalEnergy + coolingPower;

            Log.printLine("Total Energy Consumption: " + totalEnergy + " Watts");
            Log.printLine("Cooling Power Consumption: " + coolingPower + " Watts");
            Log.printLine("Total Energy Consumption with Cooling: " + totalEnergyWithCooling + " Watts");

            // Measure iteration time and total convergence time
            long iterationEndTime = System.nanoTime();
            long iterationTime = iterationEndTime - iterationStartTime;

            Log.printLine("Iteration completed in " + (iterationTime / 1_000_000) + " ms.");
            Log.printLine("Total convergence time: " + (iterationTime / 1_000_000) + " ms.");

            // Write energy results to file
            writeEnergyResultsToFile("C:\\Users\\vinay\\Cloudsim_ResearchProject\\simulation_output_aggregatedheuristicaugmentupdatednewthree.txt", totalEnergy, coolingPower, totalEnergyWithCooling);

            printCloudletList(newList);

            // Write cloudlet results to file
            writeCloudletResultsToFile(newList, "C:\\Users\\vinay\\Cloudsim_ResearchProject\\simulation_output_aggregatedheuristicaugmentupdatednewthree.txt");

            Log.printLine("CloudSim simulation finished!");

            System.out.flush();
            System.err.flush();

        } catch (Exception e) {
            e.printStackTrace();
            Log.printLine("Simulation failed due to an unexpected error.");
            System.out.flush();
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }

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

    // Simulate communication with bandwidth, penalty, and energy costs
    private static void simulateCommunicationBetweenVMs(int vm1, int vm2, double bandwidth) {
        Log.printLine("Simulating communication between VM " + vm1 + " and VM " + vm2 + " with bandwidth: " + bandwidth + " KB/s");

        double dynamicBandwidth = bandwidth * (1 + Math.random() * 0.055);    // Keep previous variation
        double bandwidthPenalty = dynamicBandwidth < 50 ? 1.21 : 1.0;         // Keep previous penalty factor
        double communicationEnergyCost = bandwidth * 1.555;                   // Slightly lower energy cost increase
        double transmissionPowerBase = 3.57;                                  // Keep previous transmission power

        totalEnergy += communicationEnergyCost;
        totalEnergy += bandwidthPenalty * communicationEnergyCost * 5;
        totalEnergy += transmissionPowerBase * 4;
    }

    private static Datacenter createDatacenter(String name, int numHosts) {
        List<Host> hostList = new ArrayList<>();

        for (int i = 0; i < numHosts; i++) {
            List<Pe> peList = new ArrayList<>();
            int mips = 1000;
            peList.add(new Pe(0, new PeProvisionerSimple(mips)));

            int ram = 8192;  
            long storage = 1000;
            int bw = 10000;

            hostList.add(new Host(
                    i,
                    new RamProvisionerSimple(ram),
                    new BwProvisionerSimple(bw),
                    storage,
                    peList,
                    new VmSchedulerTimeShared(peList)
            ));
        }

        String arch = "x86";
        String os = "Linux";
        String vmm = "Xen";

        DatacenterCharacteristics characteristics = new DatacenterCharacteristics(
                arch, os, vmm, hostList, 0.1, 0.001, 0.0001, 0.00001, 0.00001);

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

        long size = 20;
        int ram = 2;
        int mips = 50; 
        long bw = 5;
        int pesNumber = 4; 
        String vmm = "Xen";

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
                for (int i = 0; i < values.length; i++) {
                    values[i] = values[i].trim();
                }

                if (values.length < 11) {
                    System.err.println("Invalid row with insufficient columns: " + line);
                    continue;
                }

                try {
                    int cores = parseInteger(values[1], 1);
                    double cpuUsageMHz = parseDouble(values[2], 1.0);

                    long fileSize = 1;
                    long outputSize = 1;

                    Cloudlet cloudlet = new Cloudlet(
                            cloudletList.size(), (long) cpuUsageMHz * 100, cores, fileSize, outputSize,
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

    private static int parseInteger(String value, int defaultValue) {
        try {
            int result = Integer.parseInt(value);
            return (result != 0) ? result : defaultValue;
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

    // Total energy calculation
    private static double calculateTotalEnergy(List<Cloudlet> cloudlets) {
        double totalEnergy = 0.0;
        double processingPower = 0.01015;                                     // Smaller increment in processing power
        for (Cloudlet cloudlet : cloudlets) {
            totalEnergy += cloudlet.getActualCPUTime() * processingPower;
        }
        return totalEnergy;
    }

    // Cooling power calculation
    public static double calculateCoolingPower(double totalEnergyConsumption) {
    	double coolingFactor = 0.031;                                         // Keep previous cooling factor
        return totalEnergyConsumption * coolingFactor;
    }
    
    private static double calculateOverallTaskCompletionTime(List<Cloudlet> cloudlets) {
        double earliestStartTime = Double.MAX_VALUE;
        double latestFinishTime = Double.MIN_VALUE;

        for (Cloudlet cloudlet : cloudlets) {
            Vm vm = vmList.get(cloudlet.getVmId()); // Get the VM assigned to the cloudlet
            double bandwidthPenalty = 0.1; // Example value; replace with the calculated penalty
            double adjustedExecutionTime = cloudlet.getCloudletLength() / (vm.getMips() * bandwidthPenalty);

            // Update earliest start and latest finish
            if (cloudlet.getExecStartTime() < earliestStartTime) {
                earliestStartTime = cloudlet.getExecStartTime();
            }
            if (cloudlet.getFinishTime() + adjustedExecutionTime > latestFinishTime) {
                latestFinishTime = cloudlet.getFinishTime() + adjustedExecutionTime;
            }
        }
        return latestFinishTime - earliestStartTime;
    }

    private static void writeEnergyResultsToFile(String outputFileName, double totalEnergy, double coolingPower, double totalEnergyWithCooling) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(outputFileName, true))) {
            writer.println("========== ENERGY CONSUMPTION RESULTS ==========");
            writer.println("Total Energy Consumption: " + totalEnergy + " Watts");
            writer.println("Cooling Power Consumption: " + coolingPower + " Watts");
            writer.println("Total Energy Consumption with Cooling: " + totalEnergyWithCooling + " Watts");
            writer.flush();
        } catch (IOException e) {
            System.err.println("Error writing energy results to file: " + e.getMessage());
        }
    }

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
            writer.flush();
            System.out.println("Results written to: " + outputFileName);
        } catch (IOException e) {
            System.err.println("Error writing results to file: " + e.getMessage());
        }
    }
}
