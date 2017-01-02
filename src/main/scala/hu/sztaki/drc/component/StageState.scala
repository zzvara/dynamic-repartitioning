package hu.sztaki.drc.component

import hu.sztaki.drc.messages.StandaloneStrategy
import hu.sztaki.drc.{Mode, DeciderStrategy, Context, Metrics}

case class StageState[C <: Context[M], M <: Metrics[M]](
  ID: Int,
  deciderStrategy: DeciderStrategy,
  mode: Mode.Value,
  scanStrategy: StandaloneStrategy[C, M])
