/*
Copy of Pure/Build/build_schedule.scala
 */
package isabelle


import Host.Node_Info

import scala.annotation.tailrec
import scala.collection.mutable
import scala.Ordering.Implicits.seqOrdering


object Build_Schedule2 {
  /* organized historic timing information (extracted from build logs) */

  case class Result(job_name: String, hostname: String, threads: Int, timing: Timing) {
    def elapsed: Time = timing.elapsed
    def proper_cpu: Option[Time] = timing.cpu.proper_ms.map(Time.ms)
  }

  object Timing_Data {
    def median_timing(obs: List[Timing]): Timing = obs.sortBy(_.elapsed.ms).apply(obs.length / 2)

    def median_time(obs: List[Time]): Time = obs.sortBy(_.ms).apply(obs.length / 2)

    def mean_time(obs: Iterable[Time]): Time = Time.ms(obs.map(_.ms).sum / obs.size)

    private def dummy_entries(host: Host, host_factor: Double) = {
      val baseline = Time.minutes(5).scale(host_factor)
      val gc = Time.seconds(10).scale(host_factor)
      List(
        Result("dummy", host.name, 1, Timing(baseline, baseline, gc)),
        Result("dummy", host.name, 8, Timing(baseline.scale(0.2), baseline, gc)))
    }

    def make(
      host_infos: Host_Infos,
      build_history: List[(Build_Log.Meta_Info, Build_Log.Build_Info)],
      unit: Time = Time.ms(1)
    ): Timing_Data = {
      val hosts = host_infos.hosts
      val measurements =
        for {
          (meta_info, build_info) <- build_history
          build_host = meta_info.get_build_host
          (job_name, session_info) <- build_info.sessions.toList
          if build_info.finished_sessions.contains(job_name)
          hostname <- session_info.hostname.orElse(build_host).toList
          host <- hosts.find(_.name == hostname).toList
          threads = session_info.threads.getOrElse(host.max_threads)
        } yield (job_name, hostname, threads) -> session_info.timing

      val entries =
        if (measurements.isEmpty) {
          val default_host = host_infos.hosts.sorted(host_infos.host_speeds).last
          host_infos.hosts.flatMap(host =>
            dummy_entries(host, host_infos.host_factor(default_host, host)))
        }
        else
          measurements.groupMap(_._1)(_._2).toList.map {
            case ((job_name, hostname, threads), timings) =>
              Result(job_name, hostname, threads, median_timing(timings))
          }

      new Timing_Data(new Facet(entries), host_infos, unit)
    }

    def load(
      host_infos: Host_Infos,
      log_database: SQL.Database,
      unit: Time = Time.ms(1)
    ): Timing_Data = {
      val build_history =
        for {
          log_name <- log_database.execute_query_statement(
            Build_Log.private_data.meta_info_table.select(List(Build_Log.Column.log_name)),
            List.from[String], res => res.string(Build_Log.Column.log_name))
          meta_info <- Build_Log.private_data.read_meta_info(log_database, log_name)
          build_info = Build_Log.private_data.read_build_info(log_database, log_name)
        } yield (meta_info, build_info)

      make(host_infos, build_history, unit)
    }


    /* data facets */

    object Facet {
      def unapply(facet: Facet): Option[List[Result]] = Some(facet.results)
    }

    class Facet(val results: List[Result]) {
      require(results.nonEmpty)

      def is_empty: Boolean = results.isEmpty

      def size: Int = results.length

      lazy val by_job: Map[String, Facet] = results.groupBy(_.job_name).view.mapValues(new Facet(_)).toMap
      lazy val by_threads: Map[Int, Facet] = results.groupBy(_.threads).view.mapValues(new Facet(_)).toMap
      lazy val by_hostname: Map[String, Facet] = results.groupBy(_.hostname).view.mapValues(new Facet(_)).toMap

      def median_time: Time = Timing_Data.median_time(results.map(_.elapsed))

      def best_result: Result = results.minBy(_.elapsed.ms)
    }
  }

  class Timing_Data(val facet: Timing_Data.Facet, val host_infos: Host_Infos, unit: Time, use_model: Boolean = true) {
    private def inflection_point(last_mono: Int, next: Int): Int =
      last_mono + ((next - last_mono) / 2)

    def best_threads(job_name: String, max_threads: Int): Int = {
      val worse_threads =
        facet.by_job.get(job_name).toList.flatMap(_.by_hostname).flatMap {
          case (hostname, facet) =>
            val best_threads = facet.best_result.threads
            facet.by_threads.keys.toList.sorted.find(_ > best_threads).map(
              inflection_point(best_threads, _))
        }
      (max_threads :: worse_threads).min
    }

    def hostname_factor(from: String, to: String): Double =
      host_infos.host_factor(host_infos.the_host(from), host_infos.the_host(to))

    def approximate_threads(entries_unsorted: List[(Int, Time)], threads: Int): Time = {
      val entries = entries_unsorted.sortBy(_._1)

      def sorted_prefix[A](xs: List[A], f: A => Long): List[A] =
        xs match {
          case x1 :: x2 :: xs =>
            if (f(x1) <= f(x2)) x1 :: sorted_prefix(x2 :: xs, f) else x1 :: Nil
          case xs => xs
        }

      def linear(p0: (Int, Time), p1: (Int, Time)): Time = {
        val a = (p1._2 - p0._2).scale(1.0 / (p1._1 - p0._1))
        val b = p0._2 - a.scale(p0._1)
        (a.scale(threads) + b) max Time.zero
      }

      val mono_prefix = sorted_prefix(entries, e => -e._2.ms)

      val is_mono = entries == mono_prefix
      val in_prefix = mono_prefix.length > 1 && threads <= mono_prefix.last._1
      val in_inflection =
        !is_mono && mono_prefix.length > 1 && threads < entries.drop(mono_prefix.length).head._1
      if ((use_model && (is_mono || in_prefix)) || in_inflection) {
        // Model with Amdahl's law
        val t_p =
          Timing_Data.median_time(for {
            (n, t0) <- mono_prefix
            (m, t1) <- mono_prefix
            if m != n
          } yield (t0 - t1).scale(n.toDouble * m / (m - n)))
        val t_c =
          Timing_Data.median_time(for ((n, t) <- mono_prefix) yield t - t_p.scale(1.0 / n))

        def model(threads: Int): Time = (t_c + t_p.scale(1.0 / threads)) max Time.zero

        if (use_model && (is_mono || in_prefix)) model(threads)
        else {
          val post_inflection = entries.drop(mono_prefix.length).head
          val inflection_threads = inflection_point(mono_prefix.last._1, post_inflection._1)

          if (threads <= inflection_threads) model(threads)
          else linear((inflection_threads, model(inflection_threads)), post_inflection)
        }
      } else {
        // Piecewise linear
        val (p0, p1) =
          if (entries.head._1 < threads && threads < entries.last._1) {
            val split = entries.partition(_._1 < threads)
            (split._1.last, split._2.head)
          } else {
            val piece = if (threads < entries.head._1) entries.take(2) else entries.takeRight(2)
            (piece.head, piece.last)
          }

        linear(p0, p1)
      }
    }

    def unify_hosts(job_name: String, on_host: String): List[(Int, Time)] = {
      def unify(hostname: String, facet: Timing_Data.Facet) =
        facet.median_time.scale(hostname_factor(hostname, on_host))

      for {
        facet <- facet.by_job.get(job_name).toList
        (threads, facet) <- facet.by_threads
        entries = facet.by_hostname.toList.map(unify)
      } yield threads -> Timing_Data.mean_time(entries)
    }

    def estimate_threads(job_name: String, hostname: String, threads: Int): Option[Time] = {
      def try_approximate(facet: Timing_Data.Facet): Option[Time] = {
        val entries =
          facet.by_threads.toList match {
            case List((i, Timing_Data.Facet(List(result)))) if i != 1 =>
              (i, facet.median_time) :: result.proper_cpu.map(1 -> _).toList
            case entries => entries.map((threads, facet) => threads -> facet.median_time)
          }
        if (entries.size < 2) None else Some(approximate_threads(entries, threads))
      }

      for {
        facet <- facet.by_job.get(job_name)
        facet <- facet.by_hostname.get(hostname)
        time <- facet.by_threads.get(threads).map(_.median_time).orElse(try_approximate(facet))
      } yield time
    }

    def global_threads_factor(from: Int, to: Int): Double = {
      def median(xs: Iterable[Double]): Double = xs.toList.sorted.apply(xs.size / 2)

      val estimates =
        for {
          (hostname, facet) <- facet.by_hostname
          job_name <- facet.by_job.keys
          from_time <- estimate_threads(job_name, hostname, from)
          to_time <- estimate_threads(job_name, hostname, to)
        } yield from_time.ms.toDouble / to_time.ms

      if (estimates.nonEmpty) median(estimates)
      else {
        // unify hosts
        val estimates =
          for {
            (job_name, facet) <- facet.by_job
            hostname = facet.by_hostname.keys.head
            entries = unify_hosts(job_name, hostname)
            if entries.length > 1
          } yield
            approximate_threads(entries, from).ms.toDouble / approximate_threads(entries, to).ms

        if (estimates.nonEmpty) median(estimates)
        else from.toDouble / to.toDouble
      }
    }

    private var cache: Map[(String, String, Int), Time] = Map.empty


    /* approximation factors -- penalize estimations with less information */

    val FACTOR_NO_THREADS_GLOBAL_CURVE = 2.5
    val FACTOR_NO_THREADS_UNIFY_MACHINES = 1.7
    val FACTOR_NO_THREADS_OTHER_MACHINE = 1.5
    val FACTOR_NO_THREADS_SAME_MACHINE = 1.4
    val FACTOR_THREADS_OTHER_MACHINE = 1.2

    def estimate_inner(job_name: String, hostname: String, threads: Int, actual: Option[Result]): (Time, (String, Double)) = {
      def tag(time: Time, factor: String): (Time, (String, Double)) =
        actual match {
          case Some(entry) =>
            val actual = entry.timing.elapsed.ms.toDouble
            (time, (factor, Math.abs(actual - time.ms) / actual))
          case none => (time.scale(factor match {
            case "Interpolate" => FACTOR_NO_THREADS_SAME_MACHINE
            case "Interpolate Individually" => FACTOR_NO_THREADS_OTHER_MACHINE
            case "Interpolate Unified" => FACTOR_NO_THREADS_UNIFY_MACHINES
            case "Interpolate Global Curve" => FACTOR_NO_THREADS_GLOBAL_CURVE
            case "Scale Up" => FACTOR_THREADS_OTHER_MACHINE
            case _ => 1.0
          }), ("", 0.0))
        }
      {
        facet.by_job.get(job_name) match {
          case None =>
            // no data for job, take average of other jobs for given threads
            val job_estimates = facet.by_job.keys.flatMap(estimate_threads(_, hostname, threads))
            if (job_estimates.nonEmpty) tag(Timing_Data.mean_time(job_estimates), "Other Job")
            else {
              // no other job to estimate from, use global curve to approximate any other job
              val (threads1, facet1) = facet.by_threads.head
              tag(facet1.median_time.scale(global_threads_factor(threads1, threads)), "Global Curve")
            }

          case Some(facet) =>
            facet.by_threads.get(threads) match {
              case None => // interpolate threads
                estimate_threads(job_name, hostname, threads).map(tag(_,
                  "Interpolate")).getOrElse {
                  // per machine, try to approximate config for threads
                  val approximated =
                    for {
                      hostname1 <- facet.by_hostname.keys
                      estimate <- estimate_threads(job_name, hostname1, threads)
                      factor = hostname_factor(hostname1, hostname)
                    } yield estimate.scale(factor)

                  if (approximated.nonEmpty)
                    tag(Timing_Data.mean_time(approximated), "Interpolate Individually")
                  else {
                    // no single machine where config can be approximated, unify data points
                    val unified_entries = unify_hosts(job_name, hostname)

                    if (unified_entries.length > 1)
                      tag(approximate_threads(unified_entries, threads),
                        "Interpolate Unified")
                    else {
                      // only single data point, use global curve to approximate
                      val (job_threads, job_time) = unified_entries.head
                      tag(job_time.scale(global_threads_factor(job_threads, threads)),
                        "Interpolate Global Curve")
                    }
                  }
                }

              case Some(facet) => // time for job/thread exists, interpolate machine if necessary
                facet.by_hostname.get(hostname).map(_.median_time).map(tag(_, "Exact")).getOrElse {
                  tag(Timing_Data.mean_time(
                    facet.by_hostname.toList.map((hostname1, facet) =>
                      facet.median_time.scale(hostname_factor(hostname1, hostname)))),
                    "Scale Up")
                }
            }
        }
      }
    }

    def estimate(job_name: String, hostname: String, threads: Int): Time = {
      cache.get(job_name, hostname, threads) match {
        case Some(time) => time
        case None =>
          val time = Time.ms((estimate_inner(job_name, hostname, threads, None)._1.ms.toDouble / unit.ms).ceil.toLong * unit.ms)
          cache = cache + ((job_name, hostname, threads) -> time)
          time
      }
    }
  }


  /* host information */

  object Host {
    def load(options: Options, build_host: Build_Cluster.Host, host_db: SQL.Database): Host = {
      val name = build_host.name
      val info =
        isabelle.Host.read_info(host_db, name).getOrElse(error("No info for host " + quote(name)))
      val max_threads = (options ++ build_host.options).threads(default = info.num_cpus)
      val score = info.benchmark_score.getOrElse(error("No benchmark for " + quote(name)))

      Host(
        name = name,
        num_cpus = info.num_cpus,
        max_jobs = build_host.jobs,
        max_threads = max_threads,
        numa = build_host.numa,
        numa_nodes = info.numa_nodes,
        benchmark_score = score,
        options = build_host.options)
    }
  }

  case class Host(
    name: String,
    num_cpus: Int,
    max_jobs: Int,
    max_threads: Int,
    benchmark_score: Double,
    numa: Boolean = false,
    numa_nodes: List[Int] = Nil,
    options: List[Options.Spec] = Nil)

  object Host_Infos {
    def load(
      options: Options,
      build_hosts: List[Build_Cluster.Host],
      host_db: SQL.Database
    ): Host_Infos = new Host_Infos(build_hosts.map(Host.load(options, _, host_db)))
  }

  class Host_Infos(val hosts: List[Host]) {
    require(hosts.nonEmpty)

    private val by_hostname = hosts.map(host => host.name -> host).toMap

    def host_factor(from: Host, to: Host): Double =
      from.benchmark_score / to.benchmark_score

    val host_speeds: Ordering[Host] =
      Ordering.fromLessThan((host1, host2) => host_factor(host1, host2) < 1)

    def the_host(hostname: String): Host =
      by_hostname.getOrElse(hostname, error("Unknown host " + quote(hostname)))
    def the_host(node_info: Node_Info): Host = the_host(node_info.hostname)

    def num_threads(node_info: Node_Info): Int =
      if (node_info.rel_cpus.nonEmpty) node_info.rel_cpus.length
      else the_host(node_info).max_threads

    def available(state: Build_Process.State): Resources = {
      val allocated =
        state.running.values.map(_.node_info).groupMapReduce(_.hostname)(List(_))(_ ::: _)
      new Resources(this, allocated)
    }
  }


  /* offline tracking of job configurations and resource allocations */

  case class Config(job_name: String, node_info: Node_Info) {
    def job_of(start_time: Time): Build_Process.Job =
      Build_Process.Job(job_name, "", "", node_info, Date(start_time), None)
  }

  class Resources(
    val host_infos: Host_Infos,
    allocated_nodes: Map[String, List[Node_Info]]
  ) {
    def unused_nodes(host: Host, threads: Int): List[Node_Info] =
      if (!available(host, threads)) Nil
      else {
        val node = next_node(host, threads)
        node :: allocate(node).unused_nodes(host, threads)
      }

    def unused_nodes(threads: Int): List[Node_Info] =
      host_infos.hosts.flatMap(unused_nodes(_, threads))

    def allocated(host: Host): List[Node_Info] = allocated_nodes.getOrElse(host.name, Nil)

    def allocate(node_info: Node_Info): Resources = {
      val host = host_infos.the_host(node_info)
      new Resources(host_infos, allocated_nodes + (host.name -> (node_info :: allocated(host))))
    }

    def try_allocate_tasks(
      hosts: List[(Host, Int)],
      tasks: List[(Build_Process.Task, Int, Int)],
    ): (List[Config], Resources) =
      tasks match {
        case Nil => (Nil, this)
        case (task, min_threads, max_threads) :: tasks =>
          val (config, resources) =
            hosts.find((host, _) => available(host, min_threads)) match {
              case Some((host, host_max_threads)) =>
                val free_threads = host.max_threads - ((host.max_jobs - 1) * host_max_threads)
                val node_info = next_node(host, (min_threads max free_threads) min max_threads)
                (Some(Config(task.name, node_info)), allocate(node_info))
              case None => (None, this)
            }
          val (configs, resources1) = resources.try_allocate_tasks(hosts, tasks)
          (configs ++ config, resources1)
      }

    def next_node(host: Host, threads: Int): Node_Info = {
      val numa_node_num_cpus = host.num_cpus / (host.numa_nodes.length max 1)
      def explicit_cpus(node_info: Node_Info): List[Int] =
        if (node_info.rel_cpus.nonEmpty) node_info.rel_cpus else (0 until numa_node_num_cpus).toList

      val used_nodes = allocated(host).groupMapReduce(_.numa_node)(explicit_cpus)(_ ::: _)

      val available_nodes = host.numa_nodes
      val numa_node =
        if (!host.numa) None
        else available_nodes.sortBy(n => used_nodes.getOrElse(Some(n), Nil).length).headOption

      val used_cpus = used_nodes.getOrElse(numa_node, Nil).toSet
      val available_cpus = (0 until numa_node_num_cpus).filterNot(used_cpus.contains).toList

      val rel_cpus = if (available_cpus.length >= threads) available_cpus.take(threads) else Nil

      Node_Info(host.name, numa_node, rel_cpus)
    }

    def available(host: Host, threads: Int): Boolean = {
      val used = allocated(host)

      if (used.length >= host.max_jobs) false
      else {
        if (host.numa_nodes.length <= 1)
          used.map(host_infos.num_threads).sum + threads <= host.max_threads
        else {
          def node_threads(n: Int): Int =
            used.filter(_.numa_node.contains(n)).map(host_infos.num_threads).sum

          host.numa_nodes.exists(
            node_threads(_) + threads <= host.num_cpus / host.numa_nodes.length)
        }
      }
    }
  }


  /* schedule generation */

  object Schedule {
    case class Node(job_name: String, node_info: Node_Info, start: Date, duration: Time) {
      def end: Date = Date(start.time + duration)
    }

    type Graph = isabelle.Graph[String, Node]

    def init(build_uuid: String): Schedule = Schedule(build_uuid, "none", Date.now(), Graph.empty)


    /* file representation */

    def write(value: Schedule, file: Path): Unit = {
      import XML.Encode._

      def time: T[Time] = (time => long(time.ms))
      def date: T[Date] = (date => time(date.time))
      def node_info: T[Node_Info] =
        (node_info => triple(string, option(int), list(int))(
          (node_info.hostname, node_info.numa_node, node_info.rel_cpus)))
      def node: T[Node] =
        (node => pair(string, pair(node_info, pair(date, time)))(
          (node.job_name, (node.node_info, (node.start, node.duration)))))
      def schedule: T[Schedule] =
        (schedule =>
          pair(string, pair(string, pair(date, pair(Graph.encode(string, node), long))))((
            schedule.build_uuid, (schedule.generator, (schedule.start, (schedule.graph,
            schedule.serial))))))

      File.write(file, YXML.string_of_body(schedule(value)))
    }

    def read(file: Path): Schedule = {
      import XML.Decode._

      def time: T[Time] = { body => Time.ms(long(body)) }
      def date: T[Date] = { body => Date(time(body)) }
      def node_info: T[Node_Info] =
        { body =>
          val (hostname, numa_node, rel_cpus) = triple(string, option(int), list(int))(body)
          Node_Info(hostname, numa_node, rel_cpus)
        }
      val node: T[Schedule.Node] =
        { body =>
          val (job_name, (info, (start, duration))) =
            pair(string, pair(node_info, pair(date, time)))(body)
          Node(job_name, info, start, duration)
        }
      def schedule: T[Schedule] =
        { body =>
          val (build_uuid, (generator, (start, (graph, serial)))) =
            pair(string, pair(string, (pair(date, pair(Graph.decode(string, node), long)))))(body)
          Schedule(build_uuid, generator, start, graph, serial)
        }

      schedule(YXML.parse_body(File.read(file)))
    }
  }

  case class Schedule(
    build_uuid: String,
    generator: String,
    start: Date,
    graph: Schedule.Graph,
    serial: Long = 0,
  ) {
    def next_serial: Long = Build_Process.State.inc_serial(serial)

    def end: Date =
      if (graph.is_empty) start
      else graph.maximals.map(graph.get_node).map(_.end).max(Date.Ordering)

    def duration: Time = end - start
    def durations: List[Time] = graph.keys.map(graph.get_node(_).end - start)
    def message: String = "Estimated " + duration.message_hms + " build time with " + generator

    def deviation(other: Schedule): Time = Time.ms((end - other.end).ms.abs)

    def num_built(state: Build_Process.State): Int = graph.keys.count(state.results.contains)
    def elapsed(): Time = Time.now() - start.time
    def is_empty: Boolean = graph.is_empty
    def is_outdated(options: Options, state: Build_Process.State): Boolean =
      if (is_empty) true
      else elapsed() > options.seconds("build_schedule_outdated_delay")

    def next(hostname: String, state: Build_Process.State): List[String] = {
      val now = Time.now()

      val next_nodes =
        for {
          task <- state.next_ready
          if graph.defined(task.name)
          node = graph.get_node(task.name)
          if hostname == node.node_info.hostname
        } yield node

      val (ready, other) =
        next_nodes.partition(node => graph.imm_preds(node.job_name).subsetOf(state.results.keySet))

      val waiting = other.filter(_.start.time <= now)
      val running = state.running.values.toList.map(_.node_info).filter(_.hostname == hostname)

      def try_run(ready: List[Schedule.Node], next: Schedule.Node): List[Schedule.Node] = {
        val existing = ready.map(_.node_info) ::: running
        val is_distinct = existing.forall(_.rel_cpus.intersect(next.node_info.rel_cpus).isEmpty)
        if (existing.forall(_.rel_cpus.nonEmpty) && is_distinct) next :: ready else ready
      }

      waiting.foldLeft(ready)(try_run).map(_.job_name)
    }

    def exists_next(hostname: String, state: Build_Process.State): Boolean =
      next(hostname, state).nonEmpty

    def update(state: Build_Process.State): Schedule = {
      val start1 = Date.now()

      def shift_elapsed(graph: Schedule.Graph, name: String): Schedule.Graph =
        graph.map_node(name, { node =>
          val elapsed = start1 - state.running(name).start_date
          node.copy(duration = (node.duration - elapsed).max(Time.zero))
        })

      def shift_starts(graph: Schedule.Graph, name: String): Schedule.Graph =
        graph.map_node(name, { node =>
          val starts = start1 :: graph.imm_preds(node.job_name).toList.map(graph.get_node(_).end)
          node.copy(start = starts.max(Date.Ordering))
        })

      val graph0 =
        state.running.keys.foldLeft(graph.restrict(state.pending.isDefinedAt))(shift_elapsed)
      val graph1 = graph0.topological_order.foldLeft(graph0)(shift_starts)

      copy(start = start1, graph = graph1)
    }
  }

  case class State(build_state: Build_Process.State, current_time: Time, finished: Schedule) {
    def start(config: Config): State =
      copy(build_state =
        build_state.copy(running = build_state.running +
          (config.job_name -> config.job_of(current_time))))

    def step(timing_data: Timing_Data): State = {
      val remaining =
        build_state.running.values.toList.map { job =>
          val elapsed = current_time - job.start_date.time
          val threads = timing_data.host_infos.num_threads(job.node_info)
          val predicted = timing_data.estimate(job.name, job.node_info.hostname, threads)
          val remaining = if (elapsed > predicted) Time.zero else predicted - elapsed
          job -> remaining
        }

      if (remaining.isEmpty) error("Schedule step without running sessions")
      else {
        val (job, elapsed) = remaining.minBy(_._2.ms)
        val now = current_time + elapsed
        val node = Schedule.Node(job.name, job.node_info, job.start_date, now - job.start_date.time)

        val host_preds =
          for {
            name <- finished.graph.keys
            pred_node = finished.graph.get_node(name)
            if pred_node.node_info.hostname == job.node_info.hostname
            if pred_node.end.time <= node.start.time
          } yield name
        val build_preds =
          build_state.sessions.graph.imm_preds(job.name).filter(finished.graph.defined)
        val preds = build_preds ++ host_preds

        val graph = preds.foldLeft(finished.graph.new_node(job.name, node))(_.add_edge(_, job.name))

        val build_state1 = build_state.remove_running(job.name).remove_pending(job.name)
        State(build_state1, now, finished.copy(graph = graph))
      }
    }

    def is_finished: Boolean = build_state.pending.isEmpty && build_state.running.isEmpty
  }

  trait Scheduler { def schedule(build_state: Build_Process.State): Schedule }

  trait Priority_Rule { def select_next(state: Build_Process.State): List[Config] }

  case class Generation_Scheme(
    priority_rule: Priority_Rule,
    timing_data: Timing_Data,
    build_uuid: String
  ) extends Scheduler {
    def schedule(build_state: Build_Process.State): Schedule = {
      @tailrec
      def simulate(state: State): State =
        if (state.is_finished) state
        else {
          val state1 =
            priority_rule
              .select_next(state.build_state)
              .foldLeft(state)(_.start(_))
              .step(timing_data)
          simulate(state1)
        }

      val start = Date.now()
      val name = "generation scheme (" + priority_rule + ")"
      val end_state =
        simulate(State(build_state, start.time, Schedule(build_uuid, name, start, Graph.empty)))

      end_state.finished
    }
  }

  case class Optimizer(schedulers: List[Scheduler], schedules: List[Schedule]) extends Scheduler {
    require(schedulers.nonEmpty)

    def schedule(state: Build_Process.State): Schedule = {
      def main(scheduler: Scheduler): Schedule = scheduler.schedule(state)
      (Par_List.map(main, schedulers) ::: schedules.map(_.update(state))).minBy(schedule =>
        schedule.durations.map(_.ms).sorted.reverse)
    }
  }


  /* priority rules */

  class Default_Heuristic(host_infos: Host_Infos) extends Priority_Rule {
    override def toString: String = "default heuristic"

    def next_jobs(resources: Resources, sorted_jobs: List[String], host: Host): List[Config] =
      sorted_jobs.zip(resources.unused_nodes(host, host.max_threads)).map(Config(_, _))

    def select_next(state: Build_Process.State): List[Config] = {
      val sorted_jobs = state.next_ready.sortBy(_.name)(state.sessions.ordering).map(_.name)
      val resources = host_infos.available(state)

      host_infos.hosts.foldLeft((sorted_jobs, List.empty[Config])) {
        case ((jobs, res), host) =>
          val configs = next_jobs(resources, jobs, host)
          val config_jobs = configs.map(_.job_name).toSet
          (jobs.filterNot(config_jobs.contains), configs ::: res)
      }._2
    }
  }

  object Path_Time_Heuristic {
    sealed trait Critical_Criterion
    case class Absolute_Time(time: Time) extends Critical_Criterion {
      override def toString: String = "absolute time (" + time.message_hms + ")"
    }
    case class Relative_Time(factor: Double) extends Critical_Criterion {
      override def toString: String = "relative time (" + factor + ")"
    }

    sealed trait Parallel_Strategy
    case class Fixed_Thread(threads: Int) extends Parallel_Strategy {
      override def toString: String = "fixed threads (" + threads + ")"
    }
    case class Time_Based_Threads(f: Time => Int) extends Parallel_Strategy {
      override def toString: String = "time based threads"
    }

    sealed trait Host_Criterion
    case object Critical_Nodes extends Host_Criterion {
      override def toString: String = "per critical node"
    }
    case class Fixed_Fraction(fraction: Double) extends Host_Criterion {
      override def toString: String = "fixed fraction (" + fraction + ")"
    }
    case class Host_Speed(min_factor: Double) extends Host_Criterion {
      override def toString: String = "host speed (" + min_factor + ")"
    }
  }

  class Path_Time_Heuristic(
    is_critical: Path_Time_Heuristic.Critical_Criterion,
    parallel_threads: Path_Time_Heuristic.Parallel_Strategy,
    host_criterion: Path_Time_Heuristic.Host_Criterion,
    timing_data: Timing_Data,
    sessions_structure: Sessions.Structure,
    max_threads_limit: Int = 8
  ) extends Priority_Rule {
    import Path_Time_Heuristic.*

    override def toString: Node = {
      val params =
        List(
          "critical: " + is_critical,
          "parallel: " + parallel_threads,
          "fast hosts: " + host_criterion)
      "path time heuristic (" + params.mkString(", ") + ")"
    }

    /* pre-computed properties for efficient heuristic */
    val host_infos: Host_Infos = timing_data.host_infos
    val ordered_hosts: List[Host] = host_infos.hosts.sorted(host_infos.host_speeds)

    val max_threads: Int = host_infos.hosts.map(_.max_threads).max min max_threads_limit

    type Node = String
    val build_graph: Graph[Node, Sessions.Info] = sessions_structure.build_graph

    val minimals: List[Node] = build_graph.minimals
    val maximals: List[Node] = build_graph.maximals

    val best_threads: Map[Node, Int] =
      build_graph.keys.map(node => node -> timing_data.best_threads(node, max_threads)).toMap

    def best_time(node: Node): Time = {
      val host = ordered_hosts.last
      val threads = best_threads(node) min host.max_threads
      timing_data.estimate(node, host.name, threads)
    }
    val best_times: Map[Node, Time] = build_graph.keys.map(node => node -> best_time(node)).toMap

    val succs_max_time_ms: Map[Node, Long] = build_graph.node_height(best_times(_).ms)
    def max_time(node: Node): Time = Time.ms(succs_max_time_ms(node)) + best_times(node)
    def max_time(task: Build_Process.Task): Time = max_time(task.name)

    def path_times(minimals: List[Node]): Map[Node, Time] = {
      def time_ms(node: Node): Long = best_times(node).ms
      val path_times_ms = build_graph.reachable_length(time_ms, build_graph.imm_succs, minimals)
      path_times_ms.view.mapValues(Time.ms).toMap
    }

    def path_max_times(minimals: List[Node]): Map[Node, Time] =
      path_times(minimals).toList.map((node, time) => node -> (time + max_time(node))).toMap

    val node_degrees: Map[Node, Int] =
      build_graph.keys.map(node => node -> build_graph.imm_succs(node).size).toMap

    def parallel_paths(
      running: List[(Node, Time)],
      nodes: Set[Node] = build_graph.keys.toSet,
      max: Int = Int.MaxValue
    ): Int =
      if (nodes.nonEmpty && nodes.map(node_degrees.apply).max > max) max
      else {
        def start(node: Node): (Node, Time) = node -> best_times(node)

        def pass_time(elapsed: Time)(node: Node, time: Time): (Node, Time) =
          node -> (time - elapsed)

        def parallel_paths(running: Map[Node, Time]): (Int, Map[Node, Time]) =
          if (running.size >= max) (max, running)
          else if (running.isEmpty) (0, running)
          else {
            def get_next(node: Node): List[Node] =
              build_graph.imm_succs(node).intersect(nodes).filter(
                build_graph.imm_preds(_).intersect(running.keySet) == Set(node)).toList

            val (next, elapsed) = running.minBy(_._2.ms)
            val (remaining, finished) =
              running.toList.map(pass_time(elapsed)).partition(_._2 > Time.zero)

            val running1 =
              remaining.map(pass_time(elapsed)).toMap ++
                finished.map(_._1).flatMap(get_next).map(start)
            val (res, running2) = parallel_paths(running1)
            (res max running.size, running2)
          }

        parallel_paths(running.toMap)._1
      }

    def select_next(state: Build_Process.State): List[Config] = {
      val resources = host_infos.available(state)

      def best_threads(task: Build_Process.Task): Int = this.best_threads(task.name)

      val rev_ordered_hosts = ordered_hosts.reverse.map(_ -> max_threads)

      val available_nodes =
        host_infos.available(state.copy(running = Map.empty))
          .unused_nodes(max_threads)
          .sortBy(node => host_infos.the_host(node))(host_infos.host_speeds).reverse

      def remaining_time(node: Node): (Node, Time) =
        state.running.get(node) match {
          case None => node -> best_times(node)
          case Some(job) =>
            val estimate =
              timing_data.estimate(job.name, job.node_info.hostname,
                host_infos.num_threads(job.node_info))
            node -> ((Time.now() - job.start_date.time + estimate) max Time.zero)
        }

      val next_sorted = state.next_ready.sortBy(max_time(_).ms).reverse
      val is_parallelizable =
        available_nodes.length >= parallel_paths(
          state.ready.map(_.name).map(remaining_time),
          max = available_nodes.length + 1)

      if (is_parallelizable) {
        val all_tasks = next_sorted.map(task => (task, best_threads(task), best_threads(task)))
        resources.try_allocate_tasks(rev_ordered_hosts, all_tasks)._1
      }
      else {
        def is_critical(time: Time): Boolean =
          this.is_critical match {
            case Absolute_Time(threshold) => time > threshold
            case Relative_Time(factor) => time > minimals.map(max_time).maxBy(_.ms).scale(factor)
          }

        val critical_minimals = state.ready.filter(task => is_critical(max_time(task))).map(_.name)
        val critical_nodes =
          path_max_times(critical_minimals).filter((_, time) => is_critical(time)).keySet

        val (critical, other) = next_sorted.partition(task => critical_nodes.contains(task.name))

        val critical_tasks = critical.map(task => (task, best_threads(task), best_threads(task)))

        def parallel_threads(task: Build_Process.Task): Int =
          this.parallel_threads match {
            case Fixed_Thread(threads) => threads
            case Time_Based_Threads(f) => f(best_times(task.name))
          }

        val other_tasks = other.map(task => (task, parallel_threads(task), best_threads(task)))

        val max_critical_parallel =
          parallel_paths(critical_minimals.map(remaining_time), critical_nodes)
        val max_critical_hosts =
          available_nodes.take(max_critical_parallel).map(_.hostname).distinct.length

        val split =
          this.host_criterion match {
            case Critical_Nodes => max_critical_hosts
            case Fixed_Fraction(fraction) =>
              ((rev_ordered_hosts.length * fraction).ceil.toInt max 1) min max_critical_hosts
            case Host_Speed(min_factor) =>
              val best = rev_ordered_hosts.head._1.benchmark_score
              val num_fast =
                rev_ordered_hosts.count(_._1.benchmark_score >= best * min_factor)
              num_fast min max_critical_hosts
          }

        val (critical_hosts, other_hosts) = rev_ordered_hosts.splitAt(split)

        val (configs1, resources1) = resources.try_allocate_tasks(critical_hosts, critical_tasks)
        val (configs2, _) = resources1.try_allocate_tasks(other_hosts, other_tasks)

        configs1 ::: configs2
      }
    }
  }


  /* master and slave processes for scheduled build */

  class Scheduled_Build_Process(
    build_context: Build.Context,
    build_progress: Progress,
    server: SSH.Server,
  ) extends Build_Process(build_context, build_progress, server) {
    /* global state: internal var vs. external database */

    protected var _schedule: Schedule = Schedule.init(build_uuid)

    override protected def synchronized_database[A](label: String)(body: => A): A =
      synchronized {
        _build_database match {
          case None => body
          case Some(db) =>
            db.transaction_lock(Build_Schedule2.private_data.all_tables, label = label) {
              val old_state =
                Build_Process.private_data.pull_state(db, build_id, worker_uuid, _state)
              val old_schedule = Build_Schedule2.private_data.pull_schedule(db, _schedule)
              _state = old_state
              _schedule = old_schedule
              val res = body
              _state =
                Build_Process.private_data.push_state(
                  db, build_id, worker_uuid, _state, old_state)
              _schedule = Build_Schedule2.private_data.pull_schedule(db, _schedule, old_schedule)
              res
            }
        }
      }


    /* build process */

    override def next_node_info(state: Build_Process.State, session_name: String): Node_Info =
      _schedule.graph.get_node(session_name).node_info

    override def next_jobs(state: Build_Process.State): List[String] =
      if (progress.stopped || _schedule.is_empty) Nil else _schedule.next(hostname, state)

    private var _build_tick: Long = 0L

    protected override def build_action(): Boolean =
      Isabelle_Thread.interrupt_handler(_ => progress.stop()) {
        val received = build_receive(n => n.channel == Build_Process.private_data.channel)
        val ready = received.contains(Build_Schedule2.private_data.channel_ready(hostname))

        val finished = synchronized { _state.finished_running() }

        def sleep: Boolean = {
          build_delay.sleep()
          val expired = synchronized { _build_tick += 1; _build_tick % build_expire == 0 }
          expired || ready || progress.stopped
        }

        finished || sleep
      }
  }

  abstract class Scheduler_Build_Process(
    build_context: Build.Context,
    build_progress: Progress,
    server: SSH.Server,
  ) extends Scheduled_Build_Process(build_context, build_progress, server) {
    require(build_context.master)

    for (db <- _build_database) {
      Build_Schedule2.private_data.transaction_lock(
        db,
        create = true,
        label = "Scheduler_Build_Process.create"
      ) { Build_Schedule2.private_data.clean_build_schedules(db) }
      db.vacuum(Build_Schedule2.private_data.tables.list)
    }


    def init_scheduler(timing_data: Timing_Data): Scheduler


    /* global resources with common close() operation */

    private final val _log_store: Build_Log.Store = Build_Log.store(build_options)
    private final val _log_database: SQL.Database =
      try {
        val db = _log_store.open_database(server = this.server)
        _log_store.init_database(db)
        db
      }
      catch { case exn: Throwable => close(); throw exn }

    override def close(): Unit = {
      Option(_log_database).foreach(_.close())
      super.close()
    }


    /* previous results via build log */

    override def open_build_cluster(): Build_Cluster = {
      val build_cluster = super.open_build_cluster()
      build_cluster.init()

      Build_Benchmark.benchmark_requirements(build_options)

      if (build_context.worker) {
        val benchmark_options = build_options.string("build_hostname") = hostname
        Build_Benchmark.benchmark(benchmark_options, progress)
      }

      build_cluster.benchmark()
    }

    private val timing_data: Timing_Data = {
      val cluster_hosts: List[Build_Cluster.Host] =
        if (!build_context.worker) build_context.build_hosts
        else {
          val local_build_host =
            Build_Cluster.Host(
              hostname, jobs = build_context.jobs, numa = build_context.numa_shuffling)
          local_build_host :: build_context.build_hosts
        }

      val host_infos = Host_Infos.load(build_options, cluster_hosts, _host_database)
      Timing_Data.load(host_infos, _log_database)
    }
    private val scheduler = init_scheduler(timing_data)

    def write_build_log(results: Build.Results, state: Build_Process.State.Results): Unit = {
      val sessions =
        for {
          (session_name, result) <- state.toList
          if !result.current
        } yield {
          val info = build_context.sessions_structure(session_name)
          val entry =
            if (!results.cancelled(session_name)) {
              val status =
                if (result.ok) Build_Log.Session_Status.finished
                else Build_Log.Session_Status.failed

              Build_Log.Session_Entry(
                chapter = info.chapter,
                groups = info.groups,
                hostname = Some(result.node_info.hostname),
                threads = Some(timing_data.host_infos.num_threads(result.node_info)),
                start = Some(result.start_date - build_start),
                timing = result.process_result.timing,
                sources = Some(result.output_shasum.digest.toString),
                status = Some(status))
            }
            else
              Build_Log.Session_Entry(
                chapter = info.chapter,
                groups = info.groups,
                status = Some(Build_Log.Session_Status.cancelled))
          session_name -> entry
        }

      val settings =
        Build_Log.Settings.all_settings.map(_.name).map(name =>
          name -> Isabelle_System.getenv(name))
      val props =
        List(
          Build_Log.Prop.build_id.name -> build_context.build_uuid,
          Build_Log.Prop.build_engine.name -> build_context.engine.name,
          Build_Log.Prop.build_host.name -> hostname,
          Build_Log.Prop.build_start.name -> Build_Log.print_date(build_start))

      val meta_info = Build_Log.Meta_Info(props, settings)
      val build_info = Build_Log.Build_Info(sessions.toMap)
      val log_name = Build_Log.log_filename(engine = build_context.engine.name, date = build_start)

      Build_Log.private_data.update_sessions(
        _log_database, _log_store.cache.compress, log_name.file_name, build_info)
      Build_Log.private_data.update_meta_info(_log_database, log_name.file_name, meta_info)
    }


    /* build process */

    def is_current(state: Build_Process.State, session_name: String): Boolean =
      state.ancestor_results(session_name) match {
        case Some(ancestor_results) if ancestor_results.forall(_.current) =>
          val sources_shasum = state.sessions(session_name).sources_shasum

          val input_shasum =
            if (ancestor_results.isEmpty) ML_Process.bootstrap_shasum()
            else SHA1.flat_shasum(ancestor_results.map(_.output_shasum))

          val store_heap =
            build_context.build_heap || Sessions.is_pure(session_name) ||
              state.sessions.iterator.exists(_.ancestors.contains(session_name))

          store.check_output(
            _database_server, session_name,
            session_options = build_context.sessions_structure(session_name).options,
            sources_shasum = sources_shasum,
            input_shasum = input_shasum,
            fresh_build = build_context.fresh_build,
            store_heap = store_heap)._1
        case _ => false
      }

    override def next_jobs(state: Build_Process.State): List[String] =
      if (progress.stopped) state.next_ready.map(_.name)
      else if (!_schedule.is_outdated(build_options, state)) _schedule.next(hostname, state)
      else {
        val current = state.next_ready.filter(task => is_current(state, task.name))
        if (current.nonEmpty) current.map(_.name)
        else {
          val start = Time.now()

          val new_schedule = scheduler.schedule(state).update(state)
          val schedule =
            if (_schedule.is_empty) new_schedule
            else List(_schedule.update(state), new_schedule).minBy(_.end)(Date.Ordering)

          val elapsed = Time.now() - start

          val timing_msg = if (elapsed.is_relevant) " (took " + elapsed.message + ")" else ""
          progress.echo_if(
            _schedule.deviation(schedule).minutes > 1 && schedule.duration >= Time.seconds(1),
            schedule.message + timing_msg)

          _schedule = schedule
          _schedule.next(hostname, state)
        }
      }

    override def run(): Build.Results = {
      val vacuous =
        synchronized_database("Scheduler_Build_Process.init") {
          for (db <- _build_database) Build_Process.private_data.clean_build(db)
          init_unsynchronized()
          _state.pending.isEmpty
        }
      if (vacuous) {
        progress.echo_warning("Nothing to build")
        stop_build()
        Build.Results(build_context)
      }
      else {
        start_worker()
        _build_cluster.start()

        try {
          while (!finished()) {
            synchronized_database("Scheduler_Build_Process.main") {
              if (progress.stopped) _state.build_running.foreach(_.cancel())
              main_unsynchronized()
              for {
                host <- build_context.build_hosts
                if _schedule.exists_next(host.name, _state)
              } build_send(Build_Schedule2.private_data.channel_ready(host.name))
            }
            while (!build_action()) {}
          }
        }
        finally {
          _build_cluster.stop()
          stop_worker()
          stop_build()
        }

        val results = synchronized_database("Scheduler_Build_Process.result") {
          val results = for ((name, result) <- _state.results) yield name -> result.process_result
          Build.Results(build_context, results = results, other_rc = _build_cluster.rc)
        }
        write_build_log(results, _state.results)
        results
      }
    }
  }


  /** SQL data model of build schedule, extending isabelle_build database */

  object private_data extends SQL.Data("isabelle_build") {
    import Build_Process.private_data.{Base, Generic}
    /* tables */

    override lazy val tables: SQL.Tables =
      SQL.Tables(Schedules.table, Nodes.table)

    lazy val all_tables: SQL.Tables =
      SQL.Tables.list(Build_Process.private_data.tables.list ::: tables.list)

    /* notifications */

    def channel_ready(hostname: String): SQL.Notification =
      SQL.Notification(Build_Process.private_data.channel, payload = hostname)


    /* schedule */

    object Schedules {
      val build_uuid = Generic.build_uuid.make_primary_key
      val generator = SQL.Column.string("generator")
      val start = SQL.Column.date("start")
      val serial = SQL.Column.long("serial")

      val table = make_table(List(build_uuid, generator, start, serial), name = "schedules")
    }

    def read_serial(db: SQL.Database, build_uuid: String = ""): Long =
      db.execute_query_statementO[Long](
        Schedules.table.select(List(Schedules.serial.max), sql =
          SQL.where(if_proper(build_uuid, Schedules.build_uuid.equal(build_uuid)))),
          _.long(Schedules.serial)).getOrElse(0L)

    def read_scheduled_builds_domain(db: SQL.Database): Map[String, Unit] =
      db.execute_query_statement(
        Schedules.table.select(List(Schedules.build_uuid)),
        Map.from[String, Unit], res => res.string(Schedules.build_uuid) -> ())

    def read_schedules(db: SQL.Database, build_uuid: String = ""): List[Schedule] = {
      val schedules =
        db.execute_query_statement(Schedules.table.select(sql =
          SQL.where(if_proper(build_uuid, Schedules.build_uuid.equal(build_uuid)))),
          List.from[Schedule],
          { res =>
            val build_uuid = res.string(Schedules.build_uuid)
            val generator = res.string(Schedules.generator)
            val start = res.date(Schedules.start)
            val serial = res.long(Schedules.serial)
            Schedule(build_uuid, generator, start, Graph.empty, serial)
          })

      for (schedule <- schedules.sortBy(_.start)(Date.Ordering)) yield {
        val nodes = private_data.read_nodes(db, build_uuid = schedule.build_uuid)
        schedule.copy(graph = Graph.make(nodes))
      }
    }

    def write_schedule(db: SQL.Database, schedule: Schedule): Unit = {
      db.execute_statement(
        Schedules.table.delete(Schedules.build_uuid.where_equal(schedule.build_uuid)))
      db.execute_statement(Schedules.table.insert(), { stmt =>
        stmt.string(1) = schedule.build_uuid
        stmt.string(2) = schedule.generator
        stmt.date(3) = schedule.start
        stmt.long(4) = schedule.serial
      })
      update_nodes(db, schedule.build_uuid, schedule.graph.dest)
    }


    /* nodes */

    object Nodes {
      val build_uuid = Generic.build_uuid.make_primary_key
      val name = Generic.name.make_primary_key
      val succs = SQL.Column.string("succs")
      val hostname = SQL.Column.string("hostname")
      val numa_node = SQL.Column.int("numa_node")
      val rel_cpus = SQL.Column.string("rel_cpus")
      val start = SQL.Column.date("start")
      val duration = SQL.Column.long("duration")

      val table =
        make_table(
          List(build_uuid, name, succs, hostname, numa_node, rel_cpus, start, duration),
          name = "schedule_nodes")
    }

    type Nodes = List[((String, Schedule.Node), List[String])]

    def read_nodes(db: SQL.Database, build_uuid: String = ""): Nodes = {
      db.execute_query_statement(
        Nodes.table.select(sql =
          SQL.where(if_proper(build_uuid, Nodes.build_uuid.equal(build_uuid)))),
        List.from[((String, Schedule.Node), List[String])],
        { res =>
          val name = res.string(Nodes.name)
          val succs = split_lines(res.string(Nodes.succs))
          val hostname = res.string(Nodes.hostname)
          val numa_node = res.get_int(Nodes.numa_node)
          val rel_cpus = res.string(Nodes.rel_cpus)
          val start = res.date(Nodes.start)
          val duration = Time.ms(res.long(Nodes.duration))

          val node_info = Node_Info(hostname, numa_node, isabelle.Host.Range.from(rel_cpus))
          ((name, Schedule.Node(name, node_info, start, duration)), succs)
        }
      )
    }

    def update_nodes(db: SQL.Database, build_uuid: String, nodes: Nodes): Unit = {
      db.execute_statement(Nodes.table.delete(Nodes.build_uuid.where_equal(build_uuid)))
      db.execute_batch_statement(Nodes.table.insert(), batch =
        for (((name, node), succs) <- nodes) yield { (stmt: SQL.Statement) =>
          stmt.string(1) = build_uuid
          stmt.string(2) = name
          stmt.string(3) = cat_lines(succs)
          stmt.string(4) = node.node_info.hostname
          stmt.int(5) = node.node_info.numa_node
          stmt.string(6) = isabelle.Host.Range(node.node_info.rel_cpus)
          stmt.date(7) = node.start
          stmt.long(8) = node.duration.ms
        })
    }

    def pull_schedule(db: SQL.Database, old_schedule: Schedule): Build_Schedule2.Schedule = {
      val serial_db = read_serial(db)
      if (serial_db == old_schedule.serial) old_schedule
      else {
        read_schedules(db, old_schedule.build_uuid) match {
          case Nil => old_schedule
          case schedules => Library.the_single(schedules)
        }
      }
    }

    def pull_schedule(db: SQL.Database, schedule: Schedule, old_schedule: Schedule): Schedule = {
      val changed =
        schedule.generator != old_schedule.generator ||
        schedule.start != old_schedule.start ||
        schedule.graph != old_schedule.graph

      val schedule1 =
        if (changed) schedule.copy(serial = old_schedule.next_serial) else schedule
      if (schedule1.serial != schedule.serial) write_schedule(db, schedule1)

      schedule1
    }

    def remove_schedules(db: SQL.Database, remove: List[String]): Unit =
      if (remove.nonEmpty) {
        val sql = Generic.build_uuid.where_member(remove)
        db.execute_statement(SQL.MULTI(tables.map(_.delete(sql = sql))))
      }

    def clean_build_schedules(db: SQL.Database): Unit = {
      val running_builds_domain =
        db.execute_query_statement(
          Base.table.select(List(Base.build_uuid), sql = SQL.where(Base.stop.undefined)),
          Map.from[String, Unit], res => res.string(Base.build_uuid) -> ())

      val update = Library.Update.make(read_scheduled_builds_domain(db), running_builds_domain)

      remove_schedules(db, update.delete)
    }
  }


  class Build_Engine extends Build.Engine("build_schedule") {
    override def build_options(options: Options, build_cluster: Boolean = false): Options = {
      val options1 = super.build_options(options, build_cluster = build_cluster)
      if (build_cluster) options1 + "build_database_server" else options1
    }

    def scheduler(timing_data: Timing_Data, context: Build.Context): Scheduler = {
      val sessions_structure = context.sessions_structure

      val is_criticals =
        List(
          Path_Time_Heuristic.Absolute_Time(Time.minutes(5)),
          Path_Time_Heuristic.Absolute_Time(Time.minutes(10)),
          Path_Time_Heuristic.Absolute_Time(Time.minutes(20)),
          Path_Time_Heuristic.Relative_Time(0.5))
      val parallel_threads =
        List(
          Path_Time_Heuristic.Fixed_Thread(1),
          Path_Time_Heuristic.Time_Based_Threads({
            case time if time < Time.minutes(1) => 1
            case time if time < Time.minutes(5) => 4
            case _ => 8
          }))
      val machine_splits =
        List(
          Path_Time_Heuristic.Critical_Nodes,
          Path_Time_Heuristic.Fixed_Fraction(0.3),
          Path_Time_Heuristic.Host_Speed(0.9))

      val path_time_heuristics =
        for {
          is_critical <- is_criticals
          parallel <- parallel_threads
          machine_split <- machine_splits
        } yield
          Path_Time_Heuristic(is_critical, parallel, machine_split, timing_data, sessions_structure)
      val default_heuristic = Default_Heuristic(timing_data.host_infos)
      val heuristics = default_heuristic :: path_time_heuristics

      val initial_schedule_file = context.build_options.string("build_schedule_initial")
      val initial =
        proper_string(initial_schedule_file).toList.map(initial_schedule_file =>
          Schedule.read(Path.explode(initial_schedule_file)).copy(build_uuid = context.build_uuid))

      Optimizer(heuristics.map(Generation_Scheme(_, timing_data, context.build_uuid)), initial)
    }

    override def open_build_process(
      context: Build.Context,
      progress: Progress,
      server: SSH.Server
    ): Build_Process =
      if (!context.master) new Scheduled_Build_Process(context, progress, server)
      else {
        val schedule_file = context.build_options.string("build_schedule")
        if (schedule_file.isEmpty) {
          new Scheduler_Build_Process(context, progress, server) {
            def init_scheduler(timing_data: Timing_Data): Scheduler =
              scheduler(timing_data, context)
          }
        }
        else {
          val finished_schedule =
            Schedule.read(Path.explode(schedule_file)).copy(build_uuid = context.build_uuid)
          new Scheduler_Build_Process(context, progress, server) {
            def init_scheduler(timing_data: Timing_Data): Scheduler =
              (build_state: Build_Process.State) => finished_schedule
          }
        }
      }
  }
  object Build_Engine extends Build_Engine


  /* build schedule */

  def build_schedule(
    options: Options,
    build_hosts: List[Build_Cluster.Host] = Nil,
    selection: Sessions.Selection = Sessions.Selection.empty,
    progress: Progress = new Progress,
    afp_root: Option[Path] = None,
    dirs: List[Path] = Nil,
    select_dirs: List[Path] = Nil,
    infos: List[Sessions.Info] = Nil,
    numa_shuffling: Boolean = false,
    augment_options: String => List[Options.Spec] = _ => Nil,
    session_setup: (String, Session) => Unit = (_, _) => (),
    cache: Term.Cache = Term.Cache.make()
  ): Schedule = {
    Build.build_process(options, build_cluster = true, remove_builds = true)

    val store =
      Build_Engine.build_store(options, build_cluster = build_hosts.nonEmpty, cache = cache)
    val log_store = Build_Log.store(options, cache = cache)
    val build_options = store.options

    def main(
      server: SSH.Server,
      database_server: Option[SQL.Database],
      log_database: PostgreSQL.Database,
      host_database: SQL.Database
    ): Schedule = {
      val full_sessions =
        Sessions.load_structure(build_options, dirs = AFP.make_dirs(afp_root) ::: dirs,
          select_dirs = select_dirs, infos = infos, augment_options = augment_options)

      val build_deps =
        Sessions.deps(full_sessions.selection(selection), progress = progress,
          inlined_files = true).check_errors

      val build_context =
        Build.Context(store, build_deps, engine = Build_Engine, afp_root = afp_root,
          build_hosts = build_hosts, hostname = Build.hostname(build_options),
          numa_shuffling = numa_shuffling, session_setup = session_setup, master = true)

      val cluster_hosts = build_context.build_hosts

      val hosts_current =
        cluster_hosts.forall(host => isabelle.Host.read_info(host_database, host.name).isDefined)
      if (!hosts_current) {
        using(Build_Cluster.make(build_context, progress = progress).open())(_.init().benchmark())
      }

      val host_infos = Host_Infos.load(build_options, cluster_hosts, host_database)
      val timing_data = Timing_Data.load(host_infos, log_database)

      val sessions = Build_Process.Sessions.empty.init(build_context, database_server, progress)

      val build_state =
        Build_Process.State(sessions = sessions,
          pending = Map.from(sessions.iterator.map(Build_Process.Task.entry(_, build_context))))

      val scheduler = Build_Engine.scheduler(timing_data, build_context)
      def schedule_msg(res: Exn.Result[Schedule]): String =
        res match { case Exn.Res(schedule) => schedule.message case _ => "" }

      progress.echo("Building schedule...")
      Timing.timeit(scheduler.schedule(build_state), schedule_msg, output = progress.echo(_))
    }

    using(store.open_server()) { server =>
      using_optional(store.maybe_open_database_server(server = server)) { database_server =>
        using(log_store.open_database(server = server)) { log_database =>
          using(store.open_build_database(
            path = isabelle.Host.private_data.database, server = server)) { host_database =>
              main(server, database_server, log_database, host_database)
          }
        }
      }
    }
  }

  def write_schedule_graphic(schedule: Schedule, output: Path): Unit = {
    import java.awt.geom.{GeneralPath, Rectangle2D}
    import java.awt.{BasicStroke, Color, Graphics2D}

    val line_height = isabelle.graphview.Metrics.default.height
    val char_width = isabelle.graphview.Metrics.default.char_width
    val padding = isabelle.graphview.Metrics.default.space_width
    val gap = isabelle.graphview.Metrics.default.gap

    val graph = schedule.graph

    def text_width(text: String): Double = text.length * char_width

    val generator_height = line_height + padding
    val hostname_height = generator_height + line_height + padding
    def time_height(time: Time): Double = time.seconds
    def date_height(date: Date): Double = time_height(date - schedule.start)

    val hosts = graph.iterator.map(_._2._1).toList.groupBy(_.node_info.hostname)

    def node_width(node: Schedule.Node): Double = 2 * padding + text_width(node.job_name)

    case class Range(start: Double, stop: Double) {
      def proper: List[Range] = if (start < stop) List(this) else Nil
      def width: Double = stop - start
    }

    val rel_node_ranges =
      hosts.toList.flatMap { (hostname, nodes) =>
        val sorted = nodes.sortBy(node => (node.start.time.ms, node.end.time.ms, node.job_name))
        sorted.foldLeft((List.empty[Schedule.Node], Map.empty[Schedule.Node, Range])) {
          case ((nodes, allocated), node) =>
            val width = node_width(node) + padding
            val parallel = nodes.filter(_.end.time > node.start.time)
            val (last, slots) =
              parallel.sortBy(allocated(_).start).foldLeft((0D, List.empty[Range])) {
                case ((start, ranges), node1) =>
                  val node_range = allocated(node1)
                  (node_range.stop, ranges ::: Range(start, node_range.start).proper)
              }
            val start =
              (Range(last, Double.MaxValue) :: slots.filter(_.width >= width)).minBy(_.width).start
            (node :: parallel, allocated + (node -> Range(start, start + width)))
        }._2
      }.toMap

    def host_width(hostname: String) =
      2 * padding + (hosts(hostname).map(rel_node_ranges(_).stop).max max text_width(hostname))

    def graph_height(graph: Graph[String, Schedule.Node]): Double =
      date_height(graph.maximals.map(graph.get_node(_).end).maxBy(_.unix_epoch))

    val height = (hostname_height + 2 * padding + graph_height(graph)).ceil.toInt
    val (last, host_starts) =
      hosts.keys.foldLeft((0D, Map.empty[String, Double])) {
        case ((previous, starts), hostname) =>
          (previous + gap + host_width(hostname), starts + (hostname -> previous))
      }
    val width = (last - gap).ceil.toInt

    def node_start(node: Schedule.Node): Double =
      host_starts(node.node_info.hostname) + padding + rel_node_ranges(node).start

    def paint(gfx: Graphics2D): Unit = {
      gfx.setColor(Color.LIGHT_GRAY)
      gfx.fillRect(0, 0, width, height)
      gfx.setRenderingHints(isabelle.graphview.Metrics.rendering_hints)
      gfx.setFont(isabelle.graphview.Metrics.default.font)
      gfx.setStroke(new BasicStroke(1, BasicStroke.CAP_BUTT, BasicStroke.JOIN_ROUND))

      draw_string(schedule.generator + ", build time: " + schedule.duration.message_hms, padding, 0)

      def draw_host(x: Double, hostname: String): Double = {
        val nodes = hosts(hostname).map(_.job_name).toSet
        val width = host_width(hostname)
        val height = 2 * padding + graph_height(graph.restrict(nodes.contains))
        val padding1 = ((width - text_width(hostname)) / 2) max 0
        val rect = new Rectangle2D.Double(x, hostname_height, width, height)
        gfx.setColor(Color.BLACK)
        gfx.draw(rect)
        gfx.setColor(Color.GRAY)
        gfx.fill(rect)
        draw_string(hostname, x + padding1, generator_height)
        x + gap + width
      }

      def draw_string(str: String, x: Double, y: Double): Unit = {
        gfx.setColor(Color.BLACK)
        gfx.drawString(str, x.toInt, (y + line_height).toInt)
      }

      def node_rect(node: Schedule.Node): Rectangle2D.Double = {
        val x = node_start(node)
        val y = hostname_height + padding + date_height(node.start)
        val width = node_width(node)
        val height = time_height(node.duration)
        new Rectangle2D.Double(x, y, width, height)
      }

      def draw_node(node: Schedule.Node): Rectangle2D.Double = {
        val rect = node_rect(node)
        gfx.setColor(Color.BLACK)
        gfx.draw(rect)
        gfx.setColor(Color.WHITE)
        gfx.fill(rect)

        def add_text(y: Double, text: String): Double =
          if (line_height > rect.height - y || text_width(text) + 2 * padding > rect.width) y
          else {
            val padding1 = padding min ((rect.height - (y + line_height)) / 2)
            draw_string(text, rect.x + padding, rect.y + y + padding1)
            y + padding1 + line_height
          }

        val node_info = node.node_info

        val duration_str = "(" + node.duration.message_hms + ")"
        val node_str =
          "on " + proper_string(node_info.toString.stripPrefix(node_info.hostname)).getOrElse("all")
        val start_str = "Start: " + (node.start - schedule.start).message_hms

        List(node.job_name, duration_str, node_str, start_str).foldLeft(0D)(add_text)

        rect
      }

      def draw_arrow(from: Schedule.Node, to: Rectangle2D.Double, curve: Double = 10): Unit = {
        val from_rect = node_rect(from)

        val path = new GeneralPath()
        path.moveTo(from_rect.getCenterX, from_rect.getMaxY)
        path.lineTo(to.getCenterX, to.getMinY)

        gfx.setColor(Color.BLUE)
        gfx.draw(path)
      }

      hosts.keys.foldLeft(0D)(draw_host)

      graph.topological_order.foreach { job_name =>
        val node = graph.get_node(job_name)
        val rect = draw_node(node)

        for {
          pred <- graph.imm_preds(job_name).iterator
          pred_node = graph.get_node(pred)
          if node.node_info.hostname != pred_node.node_info.hostname
        } draw_arrow(pred_node, rect)
      }
    }

    val name = output.file_name
    if (File.is_png(name)) Graphics_File.write_png(output.file, paint, width, height)
    else if (File.is_pdf(name)) Graphics_File.write_pdf(output.file, paint, width, height)
    else error("Bad type of file: " + quote(name) + " (.png or .pdf expected)")
  }


  /* Isabelle tool wrapper */

  val isabelle_tool = Isabelle_Tool("build_schedule", "generate build schedule", Scala_Project.here,
    { args =>
      var afp_root: Option[Path] = None
      val base_sessions = new mutable.ListBuffer[String]
      val select_dirs = new mutable.ListBuffer[Path]
      val build_hosts = new mutable.ListBuffer[Build_Cluster.Host]
      var numa_shuffling = false
      var output_file: Option[Path] = None
      var requirements = false
      val exclude_session_groups = new mutable.ListBuffer[String]
      var all_sessions = false
      val dirs = new mutable.ListBuffer[Path]
      val session_groups = new mutable.ListBuffer[String]
      var options = Options.init(specs = Options.Spec.ISABELLE_BUILD_OPTIONS)
      var verbose = false
      val exclude_sessions = new mutable.ListBuffer[String]

      val getopts = Getopts("""
Usage: isabelle build_schedule [OPTIONS] [SESSIONS ...]

  Options are:
    -A ROOT      include AFP with given root directory (":" for """ + AFP.BASE.implode + """)
    -B NAME      include session NAME and all descendants
    -D DIR       include session directory and select its sessions
    -H HOSTS     additional cluster host specifications of the form
                 NAMES:PARAMETERS (separated by commas)
    -N           cyclic shuffling of NUMA CPU nodes (performance tuning)
    -O FILE      output file (pdf or png for image, else yxml)
    -R           refer to requirements of selected sessions
    -X NAME      exclude sessions from group NAME and all descendants
    -a           select all sessions
    -d DIR       include session directory
    -g NAME      select session group NAME
    -o OPTION    override Isabelle system OPTION (via NAME=VAL or NAME)
    -v           verbose
    -x NAME      exclude session NAME and all descendants

  Generate build schedule, but do not run actual build.
""",
        "A:" -> (arg => afp_root = Some(if (arg == ":") AFP.BASE else Path.explode(arg))),
        "B:" -> (arg => base_sessions += arg),
        "D:" -> (arg => select_dirs += Path.explode(arg)),
        "H:" -> (arg => build_hosts ++= Build_Cluster.Host.parse(Registry.global, arg)),
        "N" -> (_ => numa_shuffling = true),
        "O:" -> (arg => output_file = Some(Path.explode(arg))),
        "R" -> (_ => requirements = true),
        "X:" -> (arg => exclude_session_groups += arg),
        "a" -> (_ => all_sessions = true),
        "d:" -> (arg => dirs += Path.explode(arg)),
        "g:" -> (arg => session_groups += arg),
        "o:" -> (arg => options = options + arg),
        "v" -> (_ => verbose = true),
        "x:" -> (arg => exclude_sessions += arg))

      val sessions = getopts(args)

      val progress = new Console_Progress(verbose = verbose)

      val schedule =
        build_schedule(options,
          selection = Sessions.Selection(
            requirements = requirements,
            all_sessions = all_sessions,
            base_sessions = base_sessions.toList,
            exclude_session_groups = exclude_session_groups.toList,
            exclude_sessions = exclude_sessions.toList,
            session_groups = session_groups.toList,
            sessions = sessions),
          progress = progress,
          afp_root = afp_root,
          dirs = dirs.toList,
          select_dirs = select_dirs.toList,
          numa_shuffling = isabelle.Host.numa_check(progress, numa_shuffling),
          build_hosts = build_hosts.toList)

      output_file match {
        case Some(output_file) if !schedule.is_empty =>
          if (File.is_pdf(output_file.file_name) || File.is_png(output_file.file_name))
            write_schedule_graphic(schedule, output_file)
          else Schedule.write(schedule, output_file)
        case _ =>
      }
    })
}
