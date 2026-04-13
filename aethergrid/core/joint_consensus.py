"""AetherGrid v2.0 - Joint Consensus for Membership Changes.

Implements the Raft Joint Consensus protocol for safe membership changes.

Joint Consensus ensures that membership changes are safe by:
1. Transitioning to a "joint" configuration with both old and new members
2. Requiring majority from BOTH configurations during the transition
3. Once committed, transitioning to the new configuration alone

This prevents split-brain during membership changes.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from enum import Enum
import json


class ConfigurationState(Enum):
    """States for configuration changes."""
    STABLE = "stable"           # Single active configuration
    JOINT = "joint"             # In transition (old + new configs)
    TRANSITIONING = "transitioning"  # Moving from joint to new


@dataclass
class Configuration:
    """A cluster configuration (set of voter nodes)."""
    voters: Set[int]            # Full voting members
    learners: Set[int]          # Non-voting learners (for catch-up)
    
    def __init__(self, voters: Set[int] = None, learners: Set[int] = None):
        self.voters = voters or set()
        self.learners = learners or set()
    
    def to_dict(self) -> dict:
        return {
            "voters": list(self.voters),
            "learners": list(self.learners),
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "Configuration":
        return cls(
            voters=set(data.get("voters", [])),
            learners=set(data.get("learners", [])),
        )
    
    def majority(self) -> int:
        """Return the majority size for this configuration."""
        return len(self.voters) // 2 + 1
    
    def contains(self, node_id: int) -> bool:
        """Check if node is in this configuration."""
        return node_id in self.voters or node_id in self.learners
    
    def is_voter(self, node_id: int) -> bool:
        """Check if node is a voter in this configuration."""
        return node_id in self.voters


@dataclass
class JointConfiguration:
    """
    Joint configuration during membership change.
    
    Contains both old and new configurations.
    Decisions require majority from BOTH configurations.
    """
    old_config: Configuration
    new_config: Configuration
    index: int  # Log index where this joint config was created
    
    def to_dict(self) -> dict:
        return {
            "old_config": self.old_config.to_dict(),
            "new_config": self.new_config.to_dict(),
            "index": self.index,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> "JointConfiguration":
        return cls(
            old_config=Configuration.from_dict(data["old_config"]),
            new_config=Configuration.from_dict(data["new_config"]),
            index=data["index"],
        )
    
    def has_majority(self, votes: Set[int]) -> bool:
        """
        Check if votes constitute a majority in BOTH configurations.
        
        This is the key safety property of Joint Consensus.
        """
        old_majority = self.old_config.majority()
        new_majority = self.new_config.majority()
        
        old_votes = len(votes & self.old_config.voters)
        new_votes = len(votes & self.new_config.voters)
        
        return old_votes >= old_majority and new_votes >= new_majority
    
    def has_majority_match(self, match_index: Dict[int, int], commit_index: int) -> bool:
        """
        Check if majority from BOTH configs have matched commit_index.
        """
        old_matched = sum(
            1 for node_id in self.old_config.voters
            if match_index.get(node_id, 0) >= commit_index
        )
        
        new_matched = sum(
            1 for node_id in self.new_config.voters
            if match_index.get(node_id, 0) >= commit_index
        )
        
        return (
            old_matched >= self.old_config.majority() and
            new_matched >= self.new_config.majority()
        )


class ConfigurationManager:
    """
    Manages cluster configuration and membership changes.
    
    Implements Joint Consensus for safe configuration changes.
    """
    
    def __init__(self, initial_voters: Set[int]):
        # Current stable configuration
        self.config = Configuration(voters=initial_voters)
        
        # Joint configuration during transition
        self.joint_config: Optional[JointConfiguration] = None
        
        # State
        self.state = ConfigurationState.STABLE
        
        # Pending configuration changes
        self._pending_changes: List[dict] = []
    
    # ========== Queries ==========
    
    def get_current_config(self) -> Configuration:
        """Get the current effective configuration."""
        if self.state == ConfigurationState.JOINT and self.joint_config:
            # During joint, both configs are active
            # Return the union for iteration purposes
            return Configuration(
                voters=self.joint_config.old_config.voters | self.joint_config.new_config.voters,
                learners=self.joint_config.old_config.learners | self.joint_config.new_config.learners,
            )
        return self.config
    
    def get_voters(self) -> Set[int]:
        """Get all current voters."""
        if self.state == ConfigurationState.JOINT and self.joint_config:
            return (
                self.joint_config.old_config.voters | 
                self.joint_config.new_config.voters
            )
        return self.config.voters
    
    def is_voter(self, node_id: int) -> bool:
        """Check if a node is a voter."""
        if self.state == ConfigurationState.JOINT and self.joint_config:
            return (
                node_id in self.joint_config.old_config.voters or
                node_id in self.joint_config.new_config.voters
            )
        return node_id in self.config.voters
    
    def majority_size(self) -> int:
        """Get the majority size for the current configuration."""
        if self.state == ConfigurationState.JOINT and self.joint_config:
            # During joint, need majority from BOTH configs
            # Return the larger of the two for safety
            return max(
                self.joint_config.old_config.majority(),
                self.joint_config.new_config.majority(),
            )
        return self.config.majority()
    
    def check_election_majority(self, votes: Set[int]) -> bool:
        """
        Check if votes constitute a valid election majority.
        
        During joint consensus, need majority from BOTH configs.
        """
        if self.state == ConfigurationState.JOINT and self.joint_config:
            return self.joint_config.has_majority(votes)
        
        return len(votes & self.config.voters) >= self.config.majority()
    
    def check_commit_majority(
        self, 
        match_index: Dict[int, int], 
        commit_index: int
    ) -> bool:
        """
        Check if majority have matched the commit index.
        
        During joint consensus, need majority from BOTH configs.
        """
        if self.state == ConfigurationState.JOINT and self.joint_config:
            return self.joint_config.has_majority_match(match_index, commit_index)
        
        matched = sum(
            1 for node_id in self.config.voters
            if match_index.get(node_id, 0) >= commit_index
        )
        return matched >= self.config.majority()
    
    # ========== Configuration Changes ==========
    
    def propose_add_node(self, node_id: int, as_learner: bool = False) -> dict:
        """
        Propose adding a node to the cluster.
        
        Args:
            node_id: Node to add
            as_learner: If True, add as learner (non-voting) initially
        
        Returns:
            Configuration change command
        """
        return {
            "type": "config_change",
            "change_type": "add_node",
            "node_id": node_id,
            "as_learner": as_learner,
        }
    
    def propose_remove_node(self, node_id: int) -> dict:
        """
        Propose removing a node from the cluster.
        
        Args:
            node_id: Node to remove
        
        Returns:
            Configuration change command
        """
        return {
            "type": "config_change",
            "change_type": "remove_node",
            "node_id": node_id,
        }
    
    def apply_config_change(
        self, 
        command: dict, 
        log_index: int
    ) -> Tuple[bool, str]:
        """
        Apply a configuration change.
        
        Implements Joint Consensus:
        1. If stable, transition to joint (old + new)
        2. If joint and old config committed, transition to new only
        3. New config becomes stable
        
        Args:
            command: Configuration change command
            log_index: Log index where this change was committed
        
        Returns:
            (success, error_message)
        """
        change_type = command.get("change_type")
        node_id = command.get("node_id")
        
        if self.state == ConfigurationState.STABLE:
            # Start joint consensus transition
            
            # Create new configuration
            new_voters = set(self.config.voters)
            new_learners = set(self.config.learners)
            
            if change_type == "add_node":
                if command.get("as_learner", False):
                    new_learners.add(node_id)
                else:
                    new_voters.add(node_id)
            
            elif change_type == "remove_node":
                new_voters.discard(node_id)
                new_learners.discard(node_id)
                
                # Can't remove the last voter
                if not new_voters:
                    return False, "Cannot remove last voter"
            
            new_config = Configuration(voters=new_voters, learners=new_learners)
            
            # Create joint configuration
            self.joint_config = JointConfiguration(
                old_config=self.config,
                new_config=new_config,
                index=log_index,
            )
            
            self.state = ConfigurationState.JOINT
            
            return True, ""
        
        elif self.state == ConfigurationState.JOINT:
            # Already in joint - this should be the commit of the joint config
            # Transition to the new configuration
            
            if self.joint_config and log_index > self.joint_config.index:
                # Joint config committed, transition to new
                self.config = self.joint_config.new_config
                self.joint_config = None
                self.state = ConfigurationState.STABLE
                
                return True, ""
            
            return False, "Invalid configuration transition"
        
        return False, "Invalid state for configuration change"
    
    def can_commit_at(self, log_index: int) -> bool:
        """
        Check if we can commit at the given log index.
        
        During joint consensus, the joint config entry must be committed
        before we can transition to the new config.
        """
        if self.state == ConfigurationState.JOINT and self.joint_config:
            # The joint config entry itself can be committed
            return log_index >= self.joint_config.index
        
        return True
    
    # ========== Serialization ==========
    
    def to_dict(self) -> dict:
        """Serialize configuration state."""
        result = {
            "config": self.config.to_dict(),
            "state": self.state.value,
        }
        
        if self.joint_config:
            result["joint_config"] = self.joint_config.to_dict()
        
        return result
    
    @classmethod
    def from_dict(cls, data: dict) -> "ConfigurationManager":
        """Deserialize configuration state."""
        config = Configuration.from_dict(data["config"])
        manager = cls(initial_voters=config.voters)
        manager.config = config
        manager.state = ConfigurationState(data.get("state", "stable"))
        
        if "joint_config" in data:
            manager.joint_config = JointConfiguration.from_dict(data["joint_config"])
        
        return manager


# ============== Configuration Change Commands ==============

def create_add_node_command(node_id: int, as_learner: bool = False) -> dict:
    """Create a command to add a node."""
    return {
        "type": "config_change",
        "change_type": "add_node",
        "node_id": node_id,
        "as_learner": as_learner,
    }


def create_remove_node_command(node_id: int) -> dict:
    """Create a command to remove a node."""
    return {
        "type": "config_change",
        "change_type": "remove_node",
        "node_id": node_id,
    }


def is_config_change_command(command: dict) -> bool:
    """Check if a command is a configuration change."""
    return command.get("type") == "config_change"
