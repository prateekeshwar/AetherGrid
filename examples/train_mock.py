#!/usr/bin/env python3
"""Mock AI Training Script.

Simulates a simple training job that:
- Runs for a configurable number of epochs
- Prints progress each epoch
- Writes checkpoints
- Can succeed or fail based on args

Usage:
    python train_mock.py --epochs 5 --lr 0.001
    python train_mock.py --epochs 10 --fail-at 7  # Simulates failure
"""

import argparse
import json
import os
import random
import sys
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Mock AI Training Script")
    parser.add_argument("--epochs", type=int, default=5, help="Number of epochs")
    parser.add_argument("--lr", type=float, default=0.001, help="Learning rate")
    parser.add_argument("--batch-size", type=int, default=32, help="Batch size")
    parser.add_argument("--fail-at", type=int, default=None, help="Fail at this epoch")
    parser.add_argument("--checkpoint-dir", type=str, default="/tmp/checkpoints", help="Checkpoint directory")
    parser.add_argument("--sleep", type=float, default=0.5, help="Seconds per epoch")
    args = parser.parse_args()

    print(f"[TRAIN] Starting mock training job")
    print(f"[TRAIN] Config: epochs={args.epochs}, lr={args.lr}, batch_size={args.batch_size}")
    print(f"[TRAIN] Checkpoint dir: {args.checkpoint_dir}")

    # Create checkpoint directory
    checkpoint_dir = Path(args.checkpoint_dir)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # Simulate training loop
    metrics = {
        "epoch": 0,
        "loss": 1.0,
        "accuracy": 0.1,
        "lr": args.lr,
    }

    for epoch in range(1, args.epochs + 1):
        # Simulate failure
        if args.fail_at and epoch == args.fail_at:
            print(f"[TRAIN] ERROR: Simulated failure at epoch {epoch}")
            sys.exit(1)

        # Simulate training step
        time.sleep(args.sleep)

        # Update metrics (simulated improvement)
        metrics["epoch"] = epoch
        metrics["loss"] = max(0.01, metrics["loss"] * 0.85 + random.uniform(-0.02, 0.02))
        metrics["accuracy"] = min(0.99, metrics["accuracy"] + random.uniform(0.05, 0.15))

        print(f"[TRAIN] Epoch {epoch}/{args.epochs} - "
              f"loss: {metrics['loss']:.4f}, accuracy: {metrics['accuracy']:.4f}")

        # Save checkpoint every 2 epochs
        if epoch % 2 == 0:
            checkpoint_path = checkpoint_dir / f"checkpoint_epoch_{epoch}.json"
            with open(checkpoint_path, "w") as f:
                json.dump(metrics, f, indent=2)
            print(f"[TRAIN] Saved checkpoint: {checkpoint_path}")

    # Save final model
    final_path = checkpoint_dir / "model_final.json"
    with open(final_path, "w") as f:
        json.dump({
            "final_metrics": metrics,
            "status": "completed",
            "epochs_trained": args.epochs,
        }, f, indent=2)

    print(f"[TRAIN] Training completed successfully!")
    print(f"[TRAIN] Final model saved: {final_path}")
    print(f"[TRAIN] Final metrics: loss={metrics['loss']:.4f}, accuracy={metrics['accuracy']:.4f}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
