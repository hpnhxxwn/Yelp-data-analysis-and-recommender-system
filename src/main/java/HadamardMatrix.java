/**
 * Created by hpnhxxwn on 2017/3/1.
 */
public class HadamardMatrix {
    public static void main(String[] args) {
        int n = Integer.parseInt(args[0]);
        boolean[][] matrix = new boolean[n][n];
        matrix[0][0] = true;
        for (int i = 1; i < n; i +=i) {
            for (int j = 0; j < i; j++) {
                for (int k = 0; k < i; k++) {
                    //System.out.println("i =" + i + ", j =" + j + ", k = " + k);
                    matrix[j][k+i] = matrix[j][k];
                    matrix[j+i][k] = matrix[j][k];
                    matrix[j+i][k+i] = !matrix[j][k];
                }

            }
        }

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                if (matrix[i][j]) {
                    System.out.print("1 ");
                }
                else {
                    System.out.print("0 ");
                }
                //System.out.print(" ");
            }
            System.out.println();
        }
        return;
    }
}
