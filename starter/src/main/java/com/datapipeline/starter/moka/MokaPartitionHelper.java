package com.datapipeline.starter.moka;

import com.mokahr.dble.route.function.PatternByOrg;
import java.util.Optional;


public enum MokaPartitionHelper {
  /**
   * 分区计算单例
   **/
  INSTANSE;

  private PatternByOrg patternByOrg;


  MokaPartitionHelper() {
    PatternByOrg patternByOrg = new PatternByOrg();

    patternByOrg.setMapFile("patternbyorg.txt");

    patternByOrg.setPatternValue(1200);

    patternByOrg.setLargeOrgIds(
        FileUtil.readFileContent("/root/largeorgid.txt"));
    patternByOrg.init();
    this.patternByOrg = patternByOrg;
  }

  public Integer calculateByOrgId(String orgId) {
    return Optional.ofNullable(patternByOrg.calculate(orgId)).orElse(1);
  }

}
