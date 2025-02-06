package cloudsimresearchprojectfinal;

import java.io.*;
import java.util.*;

import org.cloudbus.cloudsim.*;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

class BaseSlice {
    int id;
    double minLatency;
    double maxLatency;
    List<Cloudlet> ueList;

    public BaseSlice(int id, double minLatency, double maxLatency) {
        this.id = id;
        this.minLatency = minLatency;
        this.maxLatency = maxLatency;
        this.ueList = new ArrayList<>();
    }
}

class BaseCluster {
    int id;
    List<Cloudlet> ueList;

    public BaseCluster(int id) {
        this.id = id;
        this.ueList = new ArrayList<>();
    }
}

public class BaseHeuristicaugment {

    private static List<Vm> vmList;
    private static List<Cloudlet> cloudletList;
    static List<BaseSlice> slices = new ArrayList<>();
    static List<BaseCluster> clusters = new ArrayList<>();

    public static void main(String[] args) {

        System.out.println("Starting CloudSim simulation...");

        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;

        try {
            PrintStream out = new PrintStream(new FileOutputStream("C:\\Users\\vinay\\Cloudsim_ResearchProject\\simulation_output_baseheuristicaugmentupdatedtwo.txt"));
            System.setOut(out);
            System.setErr(out);

            Log.setOutput(out);
            
            int num_user = 1;
            Calendar calendar = Calendar.getInstance();
            boolean trace_flag = false;

            CloudSim.init(num_user, calendar, trace_flag);

            Datacenter datacenter0 = createDatacenter("Datacenter_0", 1);
            Datacenter datacenter1 = createDatacenter("Datacenter_1", 2);
            Datacenter datacenter2 = createDatacenter("Datacenter_2", 3);

            Log.printLine(datacenter0.getName() + " created.");
            Log.printLine(datacenter1.getName() + " created.");
            Log.printLine(datacenter2.getName() + " created.");

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

            CloudSim.startSimulation();
            Log.printLine("Simulation started.");

            List<Cloudlet> newList = broker.getCloudletReceivedList();
            CloudSim.stopSimulation();
            Log.printLine("Simulation stopped. Cloudlet processing results collected.");

            double totalEnergy = calculateTotalEnergy(newList);
            double coolingPower = calculateCoolingPower(totalEnergy);
            double totalEnergyWithCooling = totalEnergy + coolingPower;

            Log.printLine("Total Energy Consumption: " + totalEnergy + " Watts");
            Log.printLine("Cooling Power Consumption: " + coolingPower + " Watts");
            Log.printLine("Total Energy Consumption with Cooling: " + totalEnergyWithCooling + " Watts");
            
            writeEnergyResultsToFile("C:\\Users\\vinay\\Cloudsim_ResearchProject\\simulation_output_baseheuristicaugmentupdatedtwo.txt", totalEnergy, coolingPower, totalEnergyWithCooling);

            printCloudletList(newList);

            writeCloudletResultsToFile(newList, "C:\\Users\\vinay\\Cloudsim_ResearchProject\\simulation_output_baseheuristicaugmentupdatedtwo.txt");

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

    private static void simulateVMCommunication() {
    	
    	// Network Slice 1
        simulateCommunicationBetweenVMs(0, 1, 5);
        simulateCommunicationBetweenVMs(1, 0, 5);
        
        // Network Slice 2
        simulateCommunicationBetweenVMs(3, 5, 7);
        simulateCommunicationBetweenVMs(3, 8, 7);
        simulateCommunicationBetweenVMs(4, 8, 7);
        simulateCommunicationBetweenVMs(4, 12, 10);
        simulateCommunicationBetweenVMs(5, 6, 7);
        simulateCommunicationBetweenVMs(5, 3, 7);
        simulateCommunicationBetweenVMs(5, 12, 10);
        
        // Network SLice 3
        simulateCommunicationBetweenVMs(7, 11, 7);
        simulateCommunicationBetweenVMs(8, 1, 7);
        simulateCommunicationBetweenVMs(8, 3, 7);
        simulateCommunicationBetweenVMs(8, 4, 7);
        simulateCommunicationBetweenVMs(11, 7, 7);
        simulateCommunicationBetweenVMs(12, 4, 10);
        simulateCommunicationBetweenVMs(12, 5, 10);
    }

    private static void simulateCommunicationBetweenVMs(int vm1, int vm2, double bandwidth) {
        Log.printLine("Simulating communication between VM " + vm1 + " and VM " + vm2 + " with bandwidth: " + bandwidth + " KB/s");
    }

    private static Datacenter createDatacenter(String name, int numHosts) {
        List<Host> hostList = new ArrayList<>();

        for (int i = 0; i < numHosts; i++) {
            List<Pe> peList = new ArrayList<>();
            int mips = 1;  // Further reduced MIPS
            peList.add(new Pe(0, new PeProvisionerSimple(mips)));

            int ram = 2;  // Further reduced Host memory (MB)
            long storage = 1000;  // Further reduced Host storage (MB)
            int bw = 20;  // Further reduced Bandwidth

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
                arch, os, vmm, hostList, 0.1, 0.001, 0.0001, 0.00001, 0.00001); // Further reduced power consumption rates

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

        long size = 20;  // Further reduced Image size (MB)
        int ram = 2;  // Further reduced VM memory (MB)
        int mips = 1;  // Further reduced MIPS value
        long bw = 5;  // Further reduced bandwidth
        int pesNumber = 1;
        String vmm = "Xen";

        for (int i = 0; i < 3; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        for (int i = 3; i < 7; i++) {
            Vm vm = new Vm(i, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerTimeShared());
            vmlist.add(vm);
        }

        for (int i = 7; i < 13; i++) {
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
                    double cpuUsageMHz = parseDouble(values[2], 1.0);  // Further reduced CPU usage MHz

                    long fileSize = 1;  // Further reduced file size
                    long outputSize = 1;  // Further reduced output size

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

 // Further fine-tuned total energy calculation
    private static double calculateTotalEnergy(List<Cloudlet> cloudlets) {
        double totalEnergy = 0.0;
        double processingPower = 0.001015;  // Smaller increase
        for (Cloudlet cloudlet : cloudlets) {
            totalEnergy += cloudlet.getActualCPUTime() * processingPower;
        }
        return totalEnergy;
    }

 // Further fine-tuned cooling power calculation
    public static double calculateCoolingPower(double totalEnergyConsumption) {
    	double coolingFactor = 0.05015;  // Smaller increase
        return totalEnergyConsumption * coolingFactor;
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