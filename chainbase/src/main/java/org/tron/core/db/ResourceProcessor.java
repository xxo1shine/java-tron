package org.tron.core.db;

import static org.tron.core.config.Parameter.ChainConstant.BLOCK_PRODUCED_INTERVAL;

import org.tron.common.utils.Commons;
import org.tron.core.capsule.AccountCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.config.Parameter.AdaptiveResourceLimitConstants;
import org.tron.core.config.Parameter.ChainConstant;
import org.tron.core.exception.AccountResourceInsufficientException;
import org.tron.core.exception.BalanceInsufficientException;
import org.tron.core.exception.ContractValidateException;
import org.tron.core.exception.TooBigTransactionResultException;
import org.tron.core.store.AccountStore;
import org.tron.core.store.DynamicPropertiesStore;
import org.tron.protos.contract.Common.ResourceCode;

abstract class ResourceProcessor {

  protected DynamicPropertiesStore dynamicPropertiesStore;
  protected AccountStore accountStore;
  protected long precision;
  protected long windowSize;
  protected long averageWindowSize;

  public ResourceProcessor(DynamicPropertiesStore dynamicPropertiesStore,
      AccountStore accountStore) {
    this.dynamicPropertiesStore = dynamicPropertiesStore;
    this.accountStore = accountStore;
    this.precision = ChainConstant.PRECISION;
    this.windowSize = ChainConstant.WINDOW_SIZE_MS / BLOCK_PRODUCED_INTERVAL;
    this.averageWindowSize =
        AdaptiveResourceLimitConstants.PERIODS_MS / BLOCK_PRODUCED_INTERVAL;
  }

  abstract void updateUsage(AccountCapsule accountCapsule);

  abstract void consume(TransactionCapsule trx, TransactionTrace trace)
      throws ContractValidateException, AccountResourceInsufficientException, TooBigTransactionResultException;

  protected long increase(long lastUsage, long usage, long lastTime, long now) {
    return increase(lastUsage, usage, lastTime, now, windowSize);
  }

  protected long increase(long lastUsage, long usage, long lastTime, long now, long windowSize) {
    long averageLastUsage = divideCeil(lastUsage * precision, windowSize);
    long averageUsage = divideCeil(usage * precision, windowSize);

    if (lastTime != now) {
      assert now > lastTime;
      if (lastTime + windowSize > now) {
        long delta = now - lastTime;
        double decay = (windowSize - delta) / (double) windowSize;
        averageLastUsage = Math.round(averageLastUsage * decay);
      } else {
        averageLastUsage = 0;
      }
    }
    averageLastUsage += averageUsage;
    return getUsage(averageLastUsage, windowSize);
  }

  public long increase(AccountCapsule accountCapsule, ResourceCode resourceCode,
                          long lastUsage, long usage, long lastTime, long now) {
    long oldWindowSize = accountCapsule.getWindowSize(resourceCode);
    /* old logic */
    long averageLastUsage = divideCeil(lastUsage * this.precision, oldWindowSize);
    long averageUsage = divideCeil(usage * this.precision, this.windowSize);

    if (lastTime != now) {
      assert now > lastTime;
      if (lastTime + oldWindowSize > now) {
        long delta = now - lastTime;
        double decay = (oldWindowSize - delta) / (double) oldWindowSize;
        averageLastUsage = Math.round(averageLastUsage * decay);
      } else {
        averageLastUsage = 0;
      }
    }
    /* new logic */
    long newUsage = getUsage(averageLastUsage, oldWindowSize) +
            getUsage(averageUsage, this.windowSize);
    if (dynamicPropertiesStore.supportUnfreezeDelay()) {
      long remainUsage = getUsage(averageLastUsage, oldWindowSize);
      if (remainUsage == 0) {
        accountCapsule.setNewWindowSize(resourceCode, this.windowSize);
        return newUsage;
      }
      long remainWindowSize = oldWindowSize - (now - lastTime);
      long newWindowSize = (remainWindowSize * remainUsage + this.windowSize * usage)
              / newUsage;
      accountCapsule.setNewWindowSize(resourceCode, newWindowSize);
    }
    return newUsage;
  }

  public long unDelegateIncrease(AccountCapsule owner, AccountCapsule receiver,
                       long transferUsage, ResourceCode resourceCode, long now) {
    long lastOwnerTime = owner.getLastConsumeTime(resourceCode);
    long lastReceiverTime = receiver.getLastConsumeTime(resourceCode);
    long ownerWindowSize = owner.getWindowSize(resourceCode);
    long receiverWindowSize = receiver.getWindowSize(resourceCode);
    long ownerUsage = owner.getUsage(resourceCode);
    // Update itself first
    ownerUsage = increase(owner, resourceCode, ownerUsage, 0, lastOwnerTime, now);

    long remainOwnerWindowSize = ownerWindowSize - (now - lastOwnerTime);
    long remainReceiverWindowSize = receiverWindowSize - (now - lastReceiverTime);
    remainOwnerWindowSize = remainOwnerWindowSize < 0 ? 0 : remainOwnerWindowSize;
    remainReceiverWindowSize = remainReceiverWindowSize < 0 ? 0 : remainReceiverWindowSize;

    long newOwnerUsage = ownerUsage + transferUsage;
    // mean ownerUsage == 0 and transferUsage == 0
    if (newOwnerUsage == 0) {
        owner.setNewWindowSize(resourceCode, this.windowSize);
      return newOwnerUsage;
    }
    // calculate new windowSize
    long newOwnerWindowSize = (ownerUsage * remainOwnerWindowSize +
            transferUsage * remainReceiverWindowSize)
            / newOwnerUsage;
    owner.setNewWindowSize(resourceCode, newOwnerWindowSize);
    return newOwnerUsage;
  }

  private long divideCeil(long numerator, long denominator) {
    return (numerator / denominator) + ((numerator % denominator) > 0 ? 1 : 0);
  }

  private long getUsage(long usage, long windowSize) {
    return usage * windowSize / precision;
  }

  protected boolean consumeFeeForBandwidth(AccountCapsule accountCapsule, long fee) {
    try {
      long latestOperationTime = dynamicPropertiesStore.getLatestBlockHeaderTimestamp();
      accountCapsule.setLatestOperationTime(latestOperationTime);
      Commons.adjustBalance(accountStore, accountCapsule, -fee);
      if (dynamicPropertiesStore.supportTransactionFeePool()) {
        dynamicPropertiesStore.addTransactionFeePool(fee);
      } else if (dynamicPropertiesStore.supportBlackHoleOptimization()) {
        dynamicPropertiesStore.burnTrx(fee);
      } else {
        Commons.adjustBalance(accountStore, accountStore.getBlackhole().createDbKey(), +fee);
      }

      return true;
    } catch (BalanceInsufficientException e) {
      return false;
    }
  }


  protected boolean consumeFeeForNewAccount(AccountCapsule accountCapsule, long fee) {
    try {
      long latestOperationTime = dynamicPropertiesStore.getLatestBlockHeaderTimestamp();
      accountCapsule.setLatestOperationTime(latestOperationTime);
      Commons.adjustBalance(accountStore, accountCapsule, -fee);
      if (dynamicPropertiesStore.supportBlackHoleOptimization()) {
        dynamicPropertiesStore.burnTrx(fee);
      } else {
        Commons.adjustBalance(accountStore, accountStore.getBlackhole().createDbKey(), +fee);
      }

      return true;
    } catch (BalanceInsufficientException e) {
      return false;
    }
  }
}
