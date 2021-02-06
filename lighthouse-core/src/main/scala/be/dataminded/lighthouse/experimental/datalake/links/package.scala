package be.dataminded.lighthouse.experimental.datalake

package object links {
  type PartitionableTypedDataLink[T] = TypedDataLink[T] with TypedPartitionable[T]

  type PartitionableUntypedDataLink = UntypedDataLink with UntypedPartitionable
}
