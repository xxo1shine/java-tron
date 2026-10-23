package org.tron.core.capsule.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.tron.common.log.DiagnosticLog;
import org.tron.common.utils.Sha256Hash;

@Getter
@Slf4j(topic = "capsule")
public class MerkleTree {

  private List<Sha256Hash> hashList;
  private List<Leaf> leaves;
  private Leaf root;

  public static MerkleTree build(List<Sha256Hash> hashList) {
    MerkleTree tree = new MerkleTree();
    tree.hashList = hashList;
    tree.leaves = new ArrayList<>();
    List<Leaf> leaves = tree.createLeaves(hashList);

    while (leaves.size() > 1) {
      leaves = tree.createParentLeaves(leaves);
    }

    tree.root = leaves.get(0);
    if (DiagnosticLog.shouldLog("pr6816_per_instance_merkle_tree_built")) {
      logger.info("@@@ pr=6816 event=per_instance_merkle_tree_built inputs={} nodes={}",
          hashList.size(), tree.leaves.size());
    }
    return tree;
  }

  private List<Leaf> createParentLeaves(List<Leaf> leaves) {
    int step = 2;
    int len = leaves.size();
    return IntStream.iterate(0, i -> i + step)
        .limit(len)
        .filter(i -> i < len)
        .mapToObj(i -> {
          Leaf right = i + 1 < len ? leaves.get(i + 1) : null;
          return createLeaf(leaves.get(i), right);
        }).collect(Collectors.toList());
  }

  private List<Leaf> createLeaves(List<Sha256Hash> hashList) {
    int step = 2;
    int len = hashList.size();
    return IntStream.iterate(0, i -> i + step)
        .limit(len)
        .filter(i -> i < len)
        .mapToObj(i -> {
          Leaf right = i + 1 < len ? createLeaf(hashList.get(i + 1)) : null;
          return createLeaf(createLeaf(hashList.get(i)), right);
        }).collect(Collectors.toList());
  }

  private Leaf createLeaf(Leaf left, Leaf right) {
    Leaf leaf = new Leaf();
    leaf.hash = right == null ? left.hash : computeHash(left.hash, right.hash);
    leaf.left = left;
    leaf.right = right;
    this.leaves.add(leaf);
    return leaf;
  }

  private Leaf createLeaf(Sha256Hash hash) {
    Leaf leaf = new Leaf();
    leaf.hash = hash;
    this.leaves.add(leaf);
    return leaf;
  }

  private Sha256Hash computeHash(Sha256Hash leftHash, Sha256Hash rightHash) {
    return Sha256Hash.of(
        leftHash.getByteString().concat(rightHash.getByteString()).toByteArray());
  }

  @Getter
  public class Leaf {

    private Sha256Hash hash;
    private Leaf left, right;
  }
}
