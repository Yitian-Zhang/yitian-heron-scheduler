package zyt.custom.cpuinfo;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		
		System.out.println("Number of cores: " + CPUInfo.getInstance().getNumberOfCores());
		System.out.println("Total speed: " + CPUInfo.getInstance().getTotalSpeed() + " Hz");
		System.out.println("Cores:");
		for (int i = 0; i < CPUInfo.getInstance().getNumberOfCores(); i++)
			System.out.println(CPUInfo.getInstance().getCoreInfo(i));

		System.out.println("total ");
	}

	/*
	Output：
	Number of cores: 4
	Totale speed: 7553941504 Hz
	Cores:
	ID: 0, model: Intel(R) Core(TM) i5-3337U CPU @ 1.80GHz, speed: 1888485376 Hz
	ID: 1, model: Intel(R) Core(TM) i5-3337U CPU @ 1.80GHz, speed: 1888485376 Hz
	ID: 2, model: Intel(R) Core(TM) i5-3337U CPU @ 1.80GHz, speed: 1888485376 Hz
	ID: 3, model: Intel(R) Core(TM) i5-3337U CPU @ 1.80GHz, speed: 1888485376 Hz
	*/

}
