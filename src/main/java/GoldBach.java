/**
 * Created by hpnhxxwn on 2017/3/1.
 */
import java.util.List;
import java.util.ArrayList;

public class GoldBach {
    public static void main(String[] args) {
        int n = Integer.parseInt(args[0]);
        boolean[] isPrime = new boolean[n];
        for (int i = 2; i < n; i++) {
            isPrime[i] = true;
        }
        for (int factor = 2; factor * factor < n; factor++) {
            if (isPrime[factor]) {
                for (int j = factor; j * factor < n; j++) {
                    isPrime[j * factor] = false;
                }
            }
        }

        //int count = 0;
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 2; i < n; i++) {
            if (isPrime[i]) {
                list.add(i);
                //count++;
            }
        }

        //int[] list = new int[count];
        int left = 0;
        int right = list.size()-1;
        while (left <= right) {
            if (list.get(left) + list.get(right) == n) {
                System.out.println(n + " = " + list.get(left) + " + " + list.get(right));
                return;
            }
            else if (list.get(left) + list.get(right) < n) {
                left++;
            }
            else {
                right--;
            }
        }
        System.out.println(n + " not expressible as sum of two primes");
    }
}
