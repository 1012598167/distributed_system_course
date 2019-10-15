package DSPPCode.hadoop.k_means;
public class KMeansImpl extends KMeans{
    @Override
     public void kMeans(String inputPath, String oldCenterPath, String newCenterPath) throws Exception {
        while (!(compareAndUpdateCenters(oldCenterPath,newCenterPath)))
        {
            runOneStep(inputPath,oldCenterPath,newCenterPath);
        }
    }




}

