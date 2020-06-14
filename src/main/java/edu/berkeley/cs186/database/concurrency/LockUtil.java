package edu.berkeley.cs186.database.concurrency;
// If you see this line, you have successfully pulled the latest changes from the skeleton for proj4!
import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType is guaranteed to be one of: S, X, NL.
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(proj4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        if (transaction == null || lockType == LockType.NL) {
          return;
        }

        if (LockType.substitutable(lockContext.getEffectiveLockType(transaction), lockType)) {
          return;
        }

        acquireLocksOnAncestors(LockType.parentLock(lockType), lockContext.parentContext(), transaction);
        acquireLock(lockContext, lockType, transaction);
    }

    private static void acquireLocksOnAncestors(LockType lockToAcquire,
                                                LockContext ancestor,
                                                TransactionContext transaction) {
      if (ancestor == null ||
          LockType.substitutable(ancestor.getEffectiveLockType(transaction), lockToAcquire)) {
        return;
      }
      LockType currLockType = ancestor.getExplicitLockType(transaction);
      lockToAcquire = resolveLockTypeToAcquire(lockToAcquire, currLockType);
      acquireLocksOnAncestors(LockType.parentLock(lockToAcquire), ancestor.parentContext(), transaction);
      acquireOrPromote(ancestor, currLockType, lockToAcquire, transaction);
    }

    private static LockType resolveLockTypeToAcquire(LockType lockTypeToAcquire, LockType heldLockType) {
      if ((lockTypeToAcquire == LockType.S && heldLockType == LockType.IX) ||
          (lockTypeToAcquire == LockType.IX && heldLockType == LockType.S)) {
            return LockType.SIX;
          }
      return lockTypeToAcquire;
    }

    private static void acquireLock(LockContext lockContext,
                                    LockType lockToAcquire,
                                    TransactionContext transaction) {
      LockType currLockType = lockContext.getExplicitLockType(transaction);
      lockToAcquire = resolveLockTypeToAcquire(lockToAcquire, currLockType);
      if (canEscalate(lockToAcquire, currLockType)) {
        blockingEscalate(lockContext, transaction);
      } else {
        acquireOrPromote(lockContext, currLockType, lockToAcquire, transaction);
      }
    }

    private static boolean canEscalate(LockType lockTypeToAcquire, LockType heldLockType) {
      if (lockTypeToAcquire == LockType.SIX && heldLockType == LockType.IX) {
        return false;
      }
      return LockType.canBeParentLock(heldLockType, lockTypeToAcquire);
    }

    private static void acquireOrPromote(LockContext lockContext,
                                  LockType currLockType,
                                  LockType lockToAcquire,
                                  TransactionContext transaction) {
      if (currLockType == LockType.NL) {
        blockingAcquire(lockContext, lockToAcquire, transaction);
      } else {
        blockingPromote(lockContext, lockToAcquire, transaction);
      }
    }

    private static void blockingAcquire(LockContext lockContext, LockType lockType, TransactionContext transaction) {
      lockContext.acquire(transaction, lockType);
      while(transaction.getBlocked()) {
        continue;
      }
    }

    private static void blockingPromote(LockContext lockContext, LockType lockType, TransactionContext transaction) {
      lockContext.promote(transaction, lockType);
      while(transaction.getBlocked()) {
        continue;
      }
    }

    private static void blockingEscalate(LockContext lockContext, TransactionContext transaction) {
      lockContext.escalate(transaction);
      while(transaction.getBlocked()) {
        continue;
      }
    }

    // TODO(proj4_part2): add helper methods as you see fit
}
