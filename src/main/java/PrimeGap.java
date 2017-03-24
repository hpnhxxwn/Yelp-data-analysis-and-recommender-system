/**
 * Created by hpnhxxwn on 2017/3/1.
 */
import java.util.List;
import java.util.ArrayList;

public class PrimeGap {
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
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 2; i < n; i++) {
            if (isPrime[i]) {
                list.add(i);
            }
        }
        int gap = 0;
        int left = 0;
        int right = 0;
        for (int i = 1; i < list.size(); i++) {
            if (list.get(i) - list.get(i-1) > gap) {
                gap = list.get(i) - list.get(i-1) + 1;
                left = list.get(i-1);
                right = list.get(i);
            }
        }
        System.out.println("Largest gap is " + gap + ", from " + left + " to " + right);
    }
}
