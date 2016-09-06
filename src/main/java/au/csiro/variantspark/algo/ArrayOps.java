package au.csiro.variantspark.algo;

public final class ArrayOps {
	private ArrayOps() {}
	
	public static int[] addEq(int[] a, int b[]) {
		for(int i = 0 ; i < a.length; i++) {
			a[i]+=b[i];
		}
		return a;
	}

	public static int[] subEq(int[] a, int b[]) {
		for(int i = 0 ; i < a.length; i++) {
			a[i]-=b[i];
		}
		return a;
	}

	public static int sum(int[] a) {
		switch (a.length) {
			case 0: return 0;
			case 1: return a[0];
			case 2: return a[0] + a[1];
			default : {
				int s = 0;
				for (int i: a) {
					s+=i;
				}
				return s;
			}
		}
	}
	
}
