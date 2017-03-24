/**
 * Created by hpnhxxwn on 2017/3/1.
 */
public class PascalTriangle {
    public static void main(String[] args) {
        int n = Integer.parseInt(args[0]);

        int[][] matrix = new int[n][n];

        for (int i = 0; i < n; i++) {
            matrix[i][0] = 1;
        }
        for (int i = 1; i < n; i++) {
            matrix[i][i] = 1;
        }

        for (int i = 2; i < n; i++) {
            for (int j = 1; j < i; j++) {
                matrix[i][j] = matrix[i-1][j-1] + matrix[i-1][j];
            }
        }

        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                System.out.print(matrix[i][j] + " ");
            }
            System.out.println();
        }
    }
}
