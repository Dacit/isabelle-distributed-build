package isabelle


import Host.Node_Info
import isabelle.Java_Monitor.ClassOf

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions

import ilog.cp.IloCP
import ilog.concert.{IloCumulFunctionExpr, IloIntervalVar}


object Build_Analysis {
  private var _version = ""
  def version = _version match { case "" => error("No version") case s => s }
  def database =
    version match {
      case "B1" => "_B"
      case "C1" => "_C"
      case "C2" => "_C"
      case s => "_" + s
    }
  def cluster =
    version match {
      case "A" => "cluster.ls21old"
      case "B" => "cluster.ls21"
      case "B1" => "cluster.ls21small"
      case "C" => "cluster.ls21-full"
      case "C1" => "cluster.ls21-smalla"
      case "C2" => "cluster.ls21-smallb"
      case "CA" => "cluster.ls21-full"
    }

  def schedule_mapping =
    version match {
      case "CA" =>
        Map(
          "heuristic_3_Slow" -> Date.Format.default.parse("21-Jun-2024 09:48:34 +0200"),
          "solve_fast_3_Slow" -> Date.Format.default.parse("19-Jun-2024 17:08:06 +0200"),
          "heuristic_2_AFP" -> Date.Format.default.parse("21-Jun-2024 12:39:01 +0200"),
          "solve_fast_2_AFP" -> Date.Format.default.parse("21-Jun-2024 12:00:51 +0200"))
    }

  def generated =
    version match {
      case "CA" => "C"
      case s => s
    }
  def generated_dir = data + Path.basic("generated" + "_" + generated)

  def mean(ls: List[Double]): Double = ls.sum / ls.length

  def median_time(timing: List[Timing]): Time =
    timing.sortBy(_.elapsed.ms).apply(timing.length / 2).elapsed

  var tools: List[Isabelle_Tool] = Nil
  def add(tool: Isabelle_Tool): Isabelle_Tool = {
    tools ::= tool
    tool
  }

  def simple_tool(name: String)(body: Progress => Unit): Isabelle_Tool =
    add(Isabelle_Tool(name, name, Scala_Project.here, { args =>
      val getopts = Getopts("Usage: isabelle _ " + name)
      val version = getopts(args) match {
        case version :: Nil => version
        case _ => getopts.usage()
      }
      _version = version
      Isabelle_System.make_directory(generated_dir)
      val progress = new Console_Progress(verbose = true)
      body(progress)
    }))

  def schedule_add_deps(schedule: Build_Schedule.Schedule, sessions_structure: Sessions.Structure): Build_Schedule.Schedule = {
    val graph0 = schedule.graph
    val by_hostname = graph0.iterator.map(_._2._1).toList.groupBy(_.node_info.hostname)
    val nodes =
      for (key <- graph0.keys) yield {
        val node = graph0.get_node(key)
        val host_succs = by_hostname(node.node_info.hostname).filter(_.start.time >= node.end.time)
        val struc_succs =
          sessions_structure
            .build_graph.all_succs(List(key)).filterNot(_ == key)
            .filter(graph0.defined).map(graph0.get_node)
        val succs = host_succs ::: struc_succs
        ((key, node), succs.map(_.job_name))
      }
    schedule.copy(graph = Graph.make(nodes))
  }


  /* db handling */

  val data = Path.explode("$EXPERIMENTS_HOME/data")
  def host_db = data + Path.basic("host" + database).db
  def log_db = data + Path.basic("log" + database).db

  def load_host_infos(db: SQL.Database): List[Host.Info] =
    Host.private_data.transaction_lock(db) {
      db.execute_query_statement(
        Host.private_data.Info.table.select(Host.private_data.Info.table.columns),
        List.from[Host.Info],
        { res =>
          val hostname = res.string(Host.private_data.Info.hostname)
          val numa_info = res.string(Host.private_data.Info.numa_info)
          val num_cpus = res.int(Host.private_data.Info.num_cpus)
          val benchmark_score = res.get_double(Host.private_data.Info.benchmark_score)

          Host.Info(hostname, Host.parse_numa_info(numa_info), num_cpus, benchmark_score)
        })
    }.filter(_.benchmark_score.isDefined)

  def load_build_history(db: SQL.Database): List[(String, Build_Log.Meta_Info, Build_Log.Build_Info)] =
    for {
      log_name <- log_names(db)
      meta_info <- Build_Log.private_data.read_meta_info(db, log_name)
      build_info = Build_Log.private_data.read_build_info(db, log_name)
    } yield (log_name, meta_info, build_info)

  def build_schedule(name: String, meta_info: Build_Log.Meta_Info, build_info: Build_Log.Build_Info): Option[Build_Schedule.Schedule] =
    Exn.capture {
      val start_date = meta_info.get_build_start.get
      val nodes =
        for ((session_name, info) <- build_info.sessions.toList) yield {
          val start = info.start.get
          val hostname = info.hostname.get
          val threads = info.threads.get
          val node_info = Host.Node_Info(hostname, None, (0 until threads).toList)
          val node = Build_Schedule.Schedule.Node(session_name, node_info, start_date + start, info.timing.elapsed)
          ((session_name, node), List.empty[String])
        }
      Build_Schedule.Schedule(name, "build", start_date, Graph.make(nodes))
    } match {
      case Exn.Res(schedule) => Some(schedule)
      case _ => None
    }

  def compare_schedules(expected: Build_Schedule.Schedule, actual: Build_Schedule.Schedule): Double = {
    val expected_set = expected.graph.keys.toSet
    val actual_set = actual.graph.keys.toSet
    val diff = expected_set.diff(actual_set).union(actual_set.diff(expected_set))
    if (diff.nonEmpty) error("Different nodes: " + diff.mkString(", "))

    val errors =
      for (key <- expected_set.toList) yield {
        val expected_start = expected.graph.get_node(key).start - expected.start
        val actual_start = actual.graph.get_node(key).start - actual.start
        (expected_start - actual_start).ms.abs.toDouble / expected_start.ms
      }

    mean(errors)
  }


  /* analysis */

  case class Args(name: String, progress: Progress, host_db: SQL.Database, log_db: SQL.Database) {
    def host_infos(hosts: List[Build_Cluster.Host]): Build_Schedule.Host_Infos =
      Build_Schedule.Host_Infos.load(Options.init(), hosts, host_db)

    def host_infos(): Build_Schedule.Host_Infos = {
      val names =
        host_db.execute_query_statement(
          Host.private_data.Info.table.select(List(Host.private_data.Info.hostname)),
          List.from[String],
          { res => res.string(Host.private_data.Info.hostname) })

      host_infos(names.map(name => Build_Cluster.Host(name, name)))
    }
  }

  def analysis_tool(name: String, body: Args => Unit, other_log_db: Option[Path] = None): Isabelle_Tool =
    simple_tool(name) { progress =>
      using(SQLite.open_database(other_log_db.getOrElse(log_db))) { log_db =>
        using(SQLite.open_database(host_db)) { host_db =>
          body(Args(name = name, progress = progress, host_db = host_db, log_db = log_db))
        }
      }
    }

  val schedule_analysis_tool = analysis_tool("schedule_analysis", { args =>
    for ((schedule_name, date) <- schedule_mapping) {
      val schedule_file = generated_dir + Path.basic("schedules") + Path.basic(schedule_name)
      val (log_name, meta_info, build_info) =
        load_build_history(args.log_db).minBy((_, meta, _) =>
          Math.abs((meta.get_build_start.get - date).ms))

      args.progress.echo("Build: " + meta_info.get_build_start.get)

      val expected = Build_Schedule.Schedule.read(schedule_file)
      val actual0 = build_schedule(log_name, meta_info, build_info).get

      if (expected.graph.keys.toSet != actual0.graph.keys.toSet) error("Different sessions")

      val actual0_start = actual0.graph.get_node("Pure").start
      val actual0_duration = actual0.end - actual0_start
      args.progress.echo("Expected: " + expected.duration.message_hms + ", actual: " + actual0_duration.message_hms)

      // per session: Start time pred/actual, len pred/actual
      val elems =
        for (session <- expected.graph.keys) yield {
          val expected_node = expected.graph.get_node(session)
          val actual_node = actual0.graph.get_node(session)

          def node_eq(node1: Host.Node_Info, node2: Host.Node_Info): Boolean =
            node1.hostname == node2.hostname && node1.rel_cpus.length == node2.rel_cpus.length && node1.numa == node2.numa && node1.numa_node == node2.numa_node
          if (!node_eq(expected_node.node_info, actual_node.node_info))
            error("Node info changed. Should be: " + expected_node.node_info + ", was: " + actual_node.node_info + " for " + session)

          val actual_start = actual_node.start - actual0_start
          val expected_start = expected_node.start - expected.start
          val actual_duration = actual_node.duration
          val expected_duration = expected_node.duration

          CSV.Record(session, expected_start.seconds, actual_start.seconds, expected_duration.seconds, actual_duration.seconds)
        }
      val header = List("session", "expected_start", "actual_start", "expected_duration", "actual_duration")
      CSV.File("schedule_analysis_" + schedule_name, header, elems).write(generated_dir)

      val expected0 = expected.copy(graph =
        Graph.make(expected.graph.keys.map(k => ((k, expected.graph.get_node(k)), Nil))))
      Build_Schedule.write_schedule_graphic(expected0, generated_dir + Path.basic(schedule_name + "_expected.pdf"))
      Build_Schedule.write_schedule_graphic(actual0, generated_dir + Path.basic(schedule_name + "_actual.pdf"))
    }
  })

  case class Build_Args(
    store: Store,
    build_engine: Build_Schedule.Build_Engine,
    build_deps: Sessions.Deps,
    afp_root: Option[Path],
    build_options: Options)

  val benchmark_sessions = List("BenchmarkA", "BenchmarkB", "benchmark_control")

  def existing_session_names(log_db: SQL.Database): List[String] = {
    log_db.execute_query_statement(
      Build_Log.private_data.sessions_table.select(
        List(Build_Log.Column.session_name),
        distinct = true),
      List.from[String],
      { res => res.string(Build_Log.Column.session_name) }).filterNot(benchmark_sessions.contains)
  }

  def log_names(log_db: SQL.Database): List[String] = {
    log_db.execute_query_statement(
      Build_Log.private_data.meta_info_table.select(List(Build_Log.Column.log_name)),
      List.from[String], res => res.string(Build_Log.Column.log_name))
  }

  case class Schedule_Args(
    name: String,
    build_context: Build.Context,
    build_state: Build_Process.State,
    timing_data: Build_Schedule.Timing_Data,
    host_infos: Build_Schedule.Host_Infos,
    previous: Boolean = true)

  case class Schedule_Config[A](name: String, data: A, problem_name: String)
  object Schedule_Config {
    def apply[A](name: String, data: A): Schedule_Config[A] = Schedule_Config(name, data, name)
  }
  def schedule_tool[A](
    name: String,
    header: List[String],
    problems: List[Schedule_Config[A]],
    body: (Problem, A, Args, Build_Args, Schedule_Args) => List[List[Any]]
  ): Isabelle_Tool =
    analysis_tool(name, { args =>
      val options =
        Options.init(specs = Options.Spec.ISABELLE_BUILD_OPTIONS) + "build_database_server" + "build_database"
      if (options.string("build_database_user").isBlank) error("No database user defined")
      val cache = Term.Cache.make()
      val build_engine = Build_Schedule.Build_Engine

      val store = build_engine.build_store(options, cache = cache)
      val build_options = store.options

      val existing = existing_session_names(args.log_db)

      var previous = true
      val res = problems.flatMap { schedule_config =>
        Build.build_process(options, build_cluster = true, remove_builds = true, force = true)
        Build.build_process(options, remove_builds = true, force = true)

        val problem = all_problems(schedule_config.problem_name)
        val build_hosts = Build_Cluster.Host.parse(Registry.global, problem.hosts)
        val host_infos = args.host_infos(build_hosts)

        val afp_root = if (problem.afp) Some(AFP.BASE) else None

        val full_sessions = Sessions.load_structure(build_options, dirs = AFP.main_dirs(afp_root))
        val timing_data = Build_Schedule.Timing_Data.load(host_infos, args.log_db, full_sessions)

        val selected_sessions =
          full_sessions.selection(
            Sessions.Selection(sessions =
              full_sessions.selection(problem.selection).build_topological_order.filter(
                existing.contains))).build_graph

        val problem_sessions: List[String] = selected_sessions.keys.filter(k =>
          !selected_sessions.is_maximal(k) ||
            timing_data.estimate(k, host_infos.hosts.max(host_infos.host_speeds).name,
              timing_data.best_threads(k, 8)) >= problem.min_time)

        val sessions_structure =
          full_sessions.selection(Sessions.Selection(sessions = problem_sessions))

        val build_deps = Sessions.deps(sessions_structure, inlined_files = true).check_errors

        val build_context =
          Build.Context(store, build_deps, afp_root = afp_root,
            build_hosts = build_hosts, hostname = Build.hostname(build_options),
            master = true)

        val sessions = Build_Process.Sessions.empty.init(build_context, None, args.progress)

        def task(session: Build_Job.Session_Context): Build_Process.Task =
          Build_Process.Task(session.name, session.deps, build_context.build_uuid)

        val build_state =
          Build_Process.State(
            sessions = sessions,
            pending = Map.from(sessions.iterator.map(Build_Process.Task.entry(_, build_context))))

        val res = body(
          problem, schedule_config.data, args,
          Build_Args(store, build_engine, build_deps, afp_root, build_options),
          Schedule_Args(schedule_config.name, build_context, build_state, timing_data,
            host_infos, previous))

        previous = res.nonEmpty
        res.map(r => CSV.Record(schedule_config.name :: r: _*))
      }
      if (header.nonEmpty) CSV.File(name, "config" :: header, res).write(generated_dir)
    })


  /* scheduling */

  case class Problem(
    name: String,
    afp: Boolean,
    selection: Sessions.Selection,
    hosts: String,
    min_time: Time = Time.zero)

  def all_problems = List(
    Problem("1_Dist", false, Sessions.Selection(all_sessions = true), cluster),
    Problem("2_AFP", true, Sessions.Selection(all_sessions = true, exclude_session_groups = List("slow")), cluster),
    Problem("3_Slow", true, Sessions.Selection(all_sessions = true, exclude_session_groups = List("very_slow")), cluster),
    Problem("4_Full", true, Sessions.Selection(all_sessions = true), cluster),
  ).map(problem => problem.name -> problem).toMap


  /** generate schedule (offline data) */

  val build_schedule_offline_tool = schedule_tool("build_schedule_offline",
    List("solve_time", "heuristic", "timing"),
    List(
      Schedule_Config("1_Dist", true),
      Schedule_Config("2_AFP", true),
      Schedule_Config("3_Slow", true),
      Schedule_Config("4_Full", true),
  ), { (problem, data, args, build_args, schedule_args) =>
    val do_write: Boolean = data
    val scheduler = build_args.build_engine.scheduler(schedule_args.timing_data, schedule_args.build_context)

    val start = Time.now()
    val schedule = scheduler.schedule(schedule_args.build_state)
    val end = Time.now()

    if (do_write) {
      Build_Schedule.Schedule.write(schedule,
        generated_dir + Path.make(List("schedules", "heuristic_" + schedule_args.name)))
    }
    val time = end - start
    val duration = schedule.duration

    args.progress.echo(schedule_args.name + ": " + duration.message_hms + " in " + time.message_hms + " (" + schedule.message + ")")

    List(List(time.seconds, schedule.message, duration.seconds))
  })


  /** cplex solver **/

  def cplex_run(tool: String, relax_problem: Boolean = false, write_schedule: Boolean = false,
    check_threads: Boolean = false)(problem: Problem, data: (List[Int], Time), args: Args,
    build_args: Build_Args, schedule_args: Schedule_Args
  ): List[List[Any]] = {
    val modes: List[Int] = data._1
    val time_limit: Time = data._2
    val sessions_structure = build_args.build_deps.sessions_structure
    val host_infos = schedule_args.host_infos
    val timing_data = schedule_args.timing_data
    val graph = sessions_structure.build_graph

    val schedule =
      build_args
        .build_engine.scheduler(schedule_args.timing_data, schedule_args.build_context)
        .schedule(schedule_args.build_state)

    if (check_threads) {
      for {
        key <- schedule.graph.keys
        node = schedule.graph.get_node(key)
        threads = node.node_info.rel_cpus.length
        if !modes.contains(threads)
      } error("Schedule needed " + threads + " threads!")
    }

    def discretize(time: Time): Int = time.ms.toInt.max(1)

    val model = new IloCP()

    type S = String
    type H = String
    type M = Int
    type T = Int

    val S = graph.keys
    val H = host_infos.hosts.map(_.name)
    def M(h: H) = modes.filter(_ <= host_infos.the_host(h).num_cpus)
    val T = discretize(schedule.duration)

    val best_times: Map[S, Long] =
      (for (s <- S) yield {
        s ->
          (for {
            h <- H
            m <- M(h)
          } yield discretize(timing_data.estimate(s, h, m))).min.toLong
      }).toMap
    val min_starts = graph.node_depth(best_times.apply)
    val succs_max_time_ms: Map[S, Long] = graph.node_height(best_times.apply)

    def ES(s: S): Int = min_starts(s).toInt
    def EC(s: S): Int = (ES(s) + best_times(s)).toInt
    def LC(s: S): Int = (T - succs_max_time_ms(s)).toInt
    def LS(s: S): Int = (LC(s) - best_times(s)).toInt

    def C_t(h: H): Int = host_infos.the_host(h).max_threads
    def C_j(h: H): Int = host_infos.the_host(h).max_jobs

    def delta(s: S, h: H, m: M): T = discretize(timing_data.estimate(s, h, m))

    // start variables
    val jobs: Map[S, IloIntervalVar] =
      (for (s <- S) yield {
        val i = model.intervalVar(s)
        i.setStartMin(ES(s))
        i.setStartMax(LS(s))
        i.setEndMin(EC(s))
        i.setEndMax(LC(s))
        (s, i)
      }).toMap

    // optional interval per configuration
    val all_intervals: List[(S, H, M, IloIntervalVar)] =
      for {
        s <- S
        h <- H
        m <- M(h)
      } yield {
        val i = jobs(s)
        val interval = model.intervalVar(delta(s, h, m), s + "/" + h + "/" + m)
        interval.setOptional()
        (s, h, m, interval)
      }

    // one running config per job
    for {
      (s, elems) <- all_intervals.groupBy(_._1)
    } model.add(model.alternative(jobs(s), elems.map(_._4).toArray))

    // resource constraints: threads and jobs
    for {
      h <- H
      on_host = all_intervals.filter(_._2 == h)
    } {
      val used_threads: IloCumulFunctionExpr = on_host.map((_, _, m, i) => model.pulse(i, m)).reduce(model.sum(_, _))
      val used_jobs: IloCumulFunctionExpr = on_host.map((_, _, _, i) => model.pulse(i, 1)).reduce(model.sum(_, _))
      model.add(model.le(used_threads, C_t(h)))
      model.add(model.le(used_jobs, C_j(h)))
    }

    // precedence constraints
    if (!relax_problem) {
      for {
        s_i <- S
        s_j <- graph.imm_succs(s_i)
      } model.add(model.endBeforeStart(jobs(s_i), jobs(s_j)))
    }

    // objective: minimize end of maximals
    val objective = model.minimize(model.max(graph.maximals.map(jobs.apply).map(model.endOf).toArray))
    model.add(objective)

    model.setParameter(IloCP.IntParam.LogVerbosity, IloCP.ParameterValues.Quiet)
    model.setParameter(IloCP.DoubleParam.TimeLimit, time_limit.seconds)

    val solved = model.solve()
    val bound = Time.ms(model.getObjBound.toLong)
    val time = Time.seconds(model.getInfo(IloCP.DoubleInfo.SolveTime))
    val res =
      if (solved) {
        val res = Time.ms(model.getObjValue.toLong)
        val status = if (res == bound) "optimal" else "feasible"
        args.progress.echo(schedule_args.name + " in " + time.message_hms + ": " + res.message_hms + " (bound: " + bound.message_hms + ")")

        val resources = new Build_Schedule.Resources(schedule_args.host_infos, Map.empty)
        val intervals =
          (for {
            (s, h, m, i) <- all_intervals
            if model.isPresent(i)
            start = model.getValue(model.startOf(i))
          } yield (s, h, m, Time.ms(start.toLong))).sortBy(_._4.ms)

        case class State(build_state: Build_Process.State) {
          def wait(date: Date): State = {
            val finished =
              build_state.running.values.toList.map { job =>
                job -> (job.start_date +
                  timing_data.estimate(
                    job.name, job.node_info.hostname, host_infos.num_threads(job.node_info)))
              }.filter((_, end_date) => date.time >= end_date.time).sortBy(_._2)(Date.Ordering)

            State(finished.foldLeft(build_state) { case (build_state, (job, _)) =>
              build_state.remove_running(job.name).remove_pending(job.name)
            })
          }

          def start(s: H, date: Date, node_info: Host.Node_Info): State =
            State(build_state.copy(running = build_state.running + (s -> Build_Process.Job(s, "", "", node_info, date, None))))

          def is_finished: Boolean = build_state.pending.isEmpty && build_state.running.isEmpty
        }

        val build_start = Date.now()
        val acc =
          intervals.foldLeft((State(schedule_args.build_state), List.empty[Build_Schedule.Schedule.Node])) {
            case ((state0, nodes), (s, h, m, start)) =>
              val state1 = state0.wait(build_start + start)
              val resources = host_infos.available(state1.build_state)
              val host = host_infos.the_host(h)
              if (!resources.available(host, m)) error("Schedule infeasible. Used: " + resources.allocated(host).mkString(",") + ", requried: " + m)
              if (!graph.imm_preds(s).forall(nodes.map(_.job_name).contains)) error("Schedule missing deps")
              val node_info = resources.next_node(host, m)
              val node =
                Build_Schedule.Schedule.Node(s, node_info, build_start + start, timing_data.estimate(s, h, m))

              (state1.start(s, build_start + start, node_info), node :: nodes)
          }
        val graph1 = Graph.make(acc._2.map(node => ((node.job_name, node), List.empty[String])))
        val schedule0 =
          Build_Schedule.Schedule("", "cplex " + time_limit.message_hms, build_start, graph1)
        val schedule = schedule_add_deps(schedule0, sessions_structure)
        val state1 = acc._1.wait(schedule.end + Time.seconds(1))
        if (!state1.is_finished) {
          val not_started = state1.build_state.pending.keySet -- state1.build_state.running.keySet
          if (not_started.nonEmpty)
            error("Schedule did not finish job. Not started: " + commas_quote(not_started.toList))
          else {
            var end = schedule.end
            var state2 = state1
            while (!state2.is_finished) {
              end += Time.seconds(1)
              state2 = state2.wait(end)
            }
            error("Schedule had different completion time. Given: " + schedule.end + ", actual: " + end)
          }
        }

        for {
          host <- host_infos.hosts
          task <- schedule.next(host.name, schedule_args.build_state)
          node = schedule.graph.get_node(task)
          _ = args.progress.echo("Starting with " + node)
        }

        if (write_schedule) {
          Isabelle_System.make_directory(generated_dir + Path.basic("schedules"))
          Build_Schedule.Schedule.write(schedule,
            generated_dir + Path.basic("schedules") + Path.basic(tool + "_" + schedule_args.name))
        }

        List(List(time.seconds, status, res.seconds, bound.seconds))
      }
      else {
        args.progress.echo(schedule_args.name + ": No solution. Bound: " + bound.message_hms + " in " + time.message_hms)
        List(List(time.seconds, "timeout", "NA", bound.seconds))
      }
    model.end()
    res
  }

  val cplex_lowerbound_tool = schedule_tool("cplex_lowerbound",
    List("solve_time", "status", "timing", "bound"),
    List(
      Schedule_Config("1_Dist", ((1 to 16).toList, Time.minutes(180))),
      Schedule_Config("2_AFP",  ((1 to 16).toList, Time.minutes(180))),
      Schedule_Config("3_Slow", ((1 to 16).toList, Time.minutes(180))),
      Schedule_Config("4_Full", ((1 to 16).toList, Time.minutes(180))),
    ),
    cplex_run("lowerbound"))

  val cplex_solve_problem_tool = schedule_tool("cplex_solve_problem",
    List("solve_time", "status", "timing", "bound"),
    List(
      Schedule_Config("1_Dist", (List(1, 8, 16), Time.minutes(180))),
      Schedule_Config("2_AFP",  (List(1, 8, 16), Time.minutes(180))),
      Schedule_Config("3_Slow", (List(1, 8, 16), Time.minutes(180))),
      Schedule_Config("4_Full", (List(1, 8, 16), Time.minutes(180))),
    ), cplex_run("solve", write_schedule = true))

  val cplex_solve_problem_fast_tool = schedule_tool("cplex_solve_problem_fast",
    List("solve_time", "status", "timing", "bound"),
    List(
      Schedule_Config("1_Dist", (List(1, 8, 16), Time.minutes(5))),
      Schedule_Config("2_AFP",  (List(1, 8, 16), Time.minutes(5))),
      Schedule_Config("3_Slow", (List(1, 8, 16), Time.minutes(5))),
      Schedule_Config("4_Full", (List(1, 8, 16), Time.minutes(5))),
    ), cplex_run("solve_fast", write_schedule = true))
}

class Experiment_Tools extends Isabelle_Scala_Tools(Build_Analysis.tools: _*)