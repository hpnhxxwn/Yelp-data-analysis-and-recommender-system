/**
 * Created by hpnhxxwn on 2017/3/1.
 */
public class DiagonalMatrix {
    public static void main(String[] args) {
        int n = Integer.parseInt(args[0]);

        int[][] matrix = new int[n][n];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < n; j++) {
                matrix[i][j] = i + 1 + j;
                System.out.print(i + 1 + j + " ");
            }
            System.out.println();
        }

        return;
    }
}
