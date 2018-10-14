package moar;

import java.util.List;

public class CostReport {

  private long cost;
  private List<CostTracker> detailCosts;

  public CostReport(final long cost, final List<CostTracker> detailCosts) {
    setCost(cost);
    setDetailCosts(detailCosts);
  }

  public long getCost() {
    return cost;
  }

  public List<CostTracker> getDetailCosts() {
    return detailCosts;
  }

  public void setCost(final long cost) {
    this.cost = cost;
  }

  public void setDetailCosts(final List<CostTracker> detailCosts) {
    this.detailCosts = detailCosts;
  }

}
