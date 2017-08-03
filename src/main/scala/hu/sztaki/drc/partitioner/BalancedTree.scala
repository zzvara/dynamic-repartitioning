package hu.sztaki.drc.partitioner

class BalancedTree(weights: Array[Double]) {
	private val root = buildTree(0, weights.length - 1)

	def getPartition(key: Double): Int = root.getPartition(key)

	private def buildTree(lower: Int, upper: Int): TreeNode = {
		if (lower + 1 == upper) {
			LeafNode(lower)
		} else {
			val cutIndex = cutInHalf(lower, upper)
			SwitchNode(weights(cutIndex), buildTree(lower, cutIndex), buildTree(cutIndex, upper))
		}
	}

	private def cutInHalf(lower: Int, upper: Int): Int = {
		val middleD = weights(upper) + weights(lower)
		val index = binarySearch(middleD / 2, lower, upper)
		if (weights(index - 1) + weights(index) <= middleD) index else index - 1
	}

	// ceil strategy
	private def binarySearch(value: Double, lower: Int, upper: Int): Int = {
		if (lower == upper) {
			lower
		} else {
			val middle = (lower + upper) / 2
			if (value <= weights(middle)) {
				binarySearch(value, lower, middle)
			} else {
				binarySearch(value, middle + 1, upper)
			}
		}
	}

	override def toString: String = {
		root.toString
	}

	sealed abstract class TreeNode {
		def getPartition(key: Double): Int

		def toStringg: String
	}

	case class LeafNode(partition: Int) extends TreeNode {
		override def getPartition(key: Double): Int = partition

		override def toStringg: String = {
			s"LeafNode(partition = $partition)"
		}

		override def toString: String = {
			s"LeafNode(partition = $partition)"
		}
	}

	case class SwitchNode(cut: Double, left: TreeNode, right: TreeNode) extends TreeNode {
		override def getPartition(key: Double): Int =
		// equal values goes to left child
			if (key <= cut) left.getPartition(key) else right.getPartition(key)

		override def toStringg: String = {
			s"SwitchNode(cut = $cut)"
		}

		override def toString: String = {
			s"$toStringg leftChild = ${left.toStringg}, rightChild = ${right.toStringg}\n" +
				s"${left.toString}\n" +
				s"${right.toString}\n"
		}
	}

}

object BalancedTreeTest {
	def main(args: Array[String]): Unit = {
		val weights = Array[Double](0, 0.3, 0.55, 0.75, 0.78, 0.81, 0.84, 0.87, 0.9, 0.93, 0.96, 0.98,
			0.99, 1.0)
		//    val weights = Array[Double](0, 1.0)


		val tree = new BalancedTree(weights)
		println(tree.toString)
		println("0.79 -> " + tree.getPartition(0.79))
		println("0.81 -> " + tree.getPartition(0.81))
		println("0.0 -> " + tree.getPartition(0.0))
		println("1.0 -> " + tree.getPartition(1.0))
	}
}
