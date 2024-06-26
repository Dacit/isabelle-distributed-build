package isabelle


import Host.Node_Info
import isabelle.Java_Monitor.ClassOf

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions


object Build_Analysis {
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
      getopts(args)
      val progress = new Console_Progress(verbose = true)
      body(progress)
    }))

  /* overhead of session imports */

  val analyze_overhead_tool = simple_tool("analyze_overhead") { progress =>
    val options = Options.init()
    val store = Store(options)
    using(store.open_server()) { server =>
      using(store.open_database_server(server)) { db =>
        val afp_root = Some(AFP.BASE)
        val full_sessions = Sessions.load_structure(options, dirs = AFP.make_dirs(afp_root))

        val build_deps =
          Sessions.deps(
            full_sessions.selection(Sessions.Selection(all_sessions = true)),
            inlined_files = true, progress = progress).check_errors

        def is_afp(session_name: String): Boolean =
          full_sessions(session_name).groups.contains("AFP")

        case class Entry(
          session_name: String,
          is_afp_session: Boolean,
          theory_name: String,
          is_proper: Boolean,
          is_afp: Boolean,
          timing: Timing)

        val timings =
          for {
            (session_name, dep) <- build_deps.session_bases.toList
            _ = progress.echo("Processing " + session_name)
            session_theories = dep.proper_session_theories.map(_.theory).toSet
            props <- store.read_theory_timings(db, session_name)
            res <-
              (props, props) match {
                case (Markup.Name(name), Markup.Timing_Properties(timing)) =>
                  val theory_session_name = full_sessions.theory_qualifier(name)

                  Some(
                    Entry(session_name, is_afp(session_name), name, session_theories.contains(name),
                      is_afp(theory_session_name), timing))
                case _ => None
              }
            } yield res

        val base = timings.filter(_.is_proper).map(_.timing.cpu.ms).sum
        val extra = timings.filterNot(_.is_proper).map(_.timing.cpu.ms).sum
        val fraction = extra.toDouble / (base + extra)
        progress.echo("Proper session theories: " + Time.ms(base).message_hms +
          ", Extra: " + Time.ms(extra).message_hms + ", Fraction: " + fraction)

        val timings0 = timings.filterNot(_.is_afp_session)
        val base0 = timings0.filter(_.is_proper).map(_.timing.cpu.ms).sum
        val extra0 = timings0.filterNot(_.is_proper).map(_.timing.cpu.ms).sum
        val fraction0 = extra0.toDouble / (base0 + extra0)
        progress.echo("Proper session theories (without AFP): " + Time.ms(base0).message_hms +
          ", Extra: " + Time.ms(extra0).message_hms + " (" + fraction0 + ")")

        val timings1 = timings.filter(_.is_afp_session)
        val base1 = timings1.filter(_.is_proper).map(_.timing.cpu.ms).sum
        val extra1 = timings1.filterNot(_.is_proper).filter(_.is_afp).map(_.timing.cpu.ms).sum
        val extra2 = timings1.filterNot(_.is_proper).filterNot(_.is_afp).map(_.timing.cpu.ms).sum
        val total1 = base1 + extra1 + extra2
        val fraction1 = extra1.toDouble / total1
        val fraction2 = extra2.toDouble / total1
        progress.echo("Proper session theories (AFP sessions only): " + Time.ms(base1).message_hms +
          ", Extra (AFP theories only): " + Time.ms(extra1).message_hms + "(" + fraction1 + "), " +
          ", Extra (distribution theories only): " + Time.ms(extra2).message_hms + "(" + fraction2 + "), " +
          ", Total: " + Time.ms(total1).message_hms)
      }
    }
  }


  /* db handling */

  val data = Path.explode("$EXPERIMENTS_HOME/data")
  def host_db = data + Path.basic("host").db
  def log_db = data + Path.basic("log").db
  val benchmark_db = data + Path.basic("benchmark").db

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


  /* analysis */

  def generated = {
    val dir = data + Path.basic("generated")
    Isabelle_System.make_directory(dir)
    dir
  }

  case class Args(name: String, progress: Progress, host_db: SQL.Database, log_db: SQL.Database,
    benchmark_db: SQL.Database) {
    def host_infos(hosts: List[Build_Cluster.Host]): Build_Schedule.Host_Infos =
      Build_Schedule.Host_Infos.load(Options.init(), hosts, host_db)

    def host_infos2(hosts: List[Build_Cluster.Host]): Build_Schedule2.Host_Infos =
      Build_Schedule2.Host_Infos.load(Options.init(), hosts, host_db)

    def host_infos2(): Build_Schedule2.Host_Infos = {
      val names =
        host_db.execute_query_statement(
          Host.private_data.Info.table.select(List(Host.private_data.Info.hostname)),
          List.from[String],
          { res => res.string(Host.private_data.Info.hostname) })

      host_infos2(names.map(name => Build_Cluster.Host(name, name)))
    }

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
          using (SQLite.open_database(benchmark_db)) { benchmark_db =>
            body(Args(name = name, progress = progress, host_db = host_db, log_db = log_db,
              benchmark_db = benchmark_db))
          }
        }
      }
    }

  case class Build_Args(
    store: Store,
    build_engine: Build_Schedule.Build_Engine,
    build_engine2: Build_Schedule2.Build_Engine,
    build_deps: Sessions.Deps,
    afp_root: Option[Path],
    build_options: Options)

  case class Config[A](
    name: String,
    data: A,
    afp: Boolean = true,
    hosts: String = "cluster.ls21old",
    selection: Sessions.Selection =
    Sessions.Selection(all_sessions = true, exclude_session_groups = List("slow")))

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

  def build_analysis_tool[A](
    name: String,
    configs: List[Config[A]],
    body: (Config[A], Args, Build_Args) => String
  ): Isabelle_Tool =
    analysis_tool(name, { args =>
      val options =
        Options.init(specs = Options.Spec.ISABELLE_BUILD_OPTIONS) + "build_database_server" + "build_database"
      if (options.string("build_database_user").isBlank) error("No database user defined")

      val cache = Term.Cache.make()
      val build_engine = Build_Schedule.Build_Engine
      val build_engine2 = Build_Schedule2.Build_Engine

      val store = build_engine2.build_store(options, cache = cache)
      val build_options = store.options

      val session_names = existing_session_names(args.log_db)
      val existing = Sessions.Selection(sessions = session_names.filterNot(_.isBlank))

      val res = configs.map { config =>
        val afp_root = if (config.afp) Some(AFP.BASE) else None
        val full_sessions0 = Sessions.load_structure(build_options, dirs = AFP.make_dirs(afp_root))
        val full_sessions = if (!config.afp) full_sessions0 else full_sessions0.selection(existing)

        val build_deps =
          Sessions.deps(
            full_sessions.selection(config.selection),
            inlined_files = true).check_errors

        CSV.Record(config.name,
          body(config, args, Build_Args(store, build_engine, build_engine2, build_deps, afp_root, build_options)))
      }
      CSV.File(name, List("config", "result"), res).write(generated)
    })

  case class Schedule_Args(
    name: String,
    build_context: Build.Context,
    build_state: Build_Process.State,
    timing_data: Build_Schedule.Timing_Data,
    timing_data2: Build_Schedule2.Timing_Data,
    host_infos: Build_Schedule.Host_Infos,
    host_infos2: Build_Schedule2.Host_Infos,
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
      val build_engine2 = Build_Schedule2.Build_Engine

      val store = build_engine2.build_store(options, cache = cache)
      val build_options = store.options

      val existing = existing_session_names(args.log_db)

      var previous = true
      val res = problems.flatMap { schedule_config =>
        Build.build_process(options, build_cluster = true, remove_builds = true, force = true)
        Build.build_process(options, remove_builds = true, force = true)

        val problem = all_problems(schedule_config.problem_name)
        val build_hosts = Build_Cluster.Host.parse(Registry.global, problem.hosts)
        val host_infos = args.host_infos(build_hosts)
        val host_infos2 = args.host_infos2(build_hosts)
        val timing_data2 = Build_Schedule2.Timing_Data.load(host_infos2, args.log_db, problem.unit)

        val afp_root = if (problem.afp) Some(AFP.BASE) else None

        val full_sessions = Sessions.load_structure(build_options, dirs = AFP.make_dirs(afp_root))

        val selected_sessions =
          full_sessions.selection(
            Sessions.Selection(sessions =
              full_sessions.selection(problem.selection).build_topological_order.filter(
                existing.contains))).build_graph

        val problem_sessions = selected_sessions.keys.filter(k =>
          !selected_sessions.is_maximal(k) ||
            timing_data2.estimate(k, host_infos2.hosts.max(host_infos2.host_speeds).name,
              timing_data2.best_threads(k, 8)) >= problem.min_time)

        val sessions_structure =
          full_sessions.selection(Sessions.Selection(sessions = problem_sessions))
        val timing_data = Build_Schedule.Timing_Data.load(host_infos, args.log_db, sessions_structure)

        val build_deps = Sessions.deps(sessions_structure, inlined_files = true).check_errors

        val build_context =
          Build.Context(store, build_deps, build_engine2, afp_root = afp_root,
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
          Build_Args(store, build_engine, build_engine2, build_deps, afp_root, build_options),
          Schedule_Args(schedule_config.name, build_context, build_state, timing_data, timing_data2,
            host_infos, host_infos2, previous))

        previous = res.nonEmpty
        res.map(r => CSV.Record(schedule_config.name :: r: _*))
      }
      if (header.nonEmpty) CSV.File(name, "config" :: header, res).write(generated)
    })


  /** analyze interpolation kinds / factors **/

  val interpolation_factors_tool = analysis_tool("interpolation_factors", { args =>
    val host_infos2 = args.host_infos2()
    val entries = Build_Schedule2.Timing_Data.load(host_infos2, args.log_db).facet.results

    args.progress.echo("Processing " + entries.length + " points...")
    val res =
      for (point <- entries) yield {
        val facet1 = new Build_Schedule2.Timing_Data.Facet(entries.filterNot(_ == point))
        val timing_data1: Build_Schedule2.Timing_Data = new Build_Schedule2.Timing_Data(facet1, host_infos2, Time.ms(1))
        timing_data1.estimate_inner(point.job_name, point.hostname, point.threads, Some(point))._2
      }
    val csv =
      for {
        (kind, values) <- res.groupMap(_._1)(_._2)
        avg = values.sum / values.length
        _ = args.progress.echo(kind + "(" + values.length + "):" + (values.min, avg, values.max))
        value <- values
      } yield CSV.Record(kind, value)

    CSV.File(args.name, List("kind", "error"), csv.toList).write(generated)
  })


  /** Analyze session best for benchmark **/

  val host_factors_tool = build_analysis_tool[Boolean]("host_factors", List(Config("default", false)),
  { (config, args, build_args) =>
    val sessions_structure = build_args.build_deps.sessions_structure
    val host_infos = args.host_infos2()
    val timing_data = Build_Schedule2.Timing_Data.load(host_infos, args.log_db, Time.ms(1))

    val graph = sessions_structure.build_graph

    val pth = new Build_Schedule2.Path_Time_Heuristic(
      Build_Schedule2.Path_Time_Heuristic.Relative_Time(1D),
      Build_Schedule2.Path_Time_Heuristic.Fixed_Thread(1), Build_Schedule2.Path_Time_Heuristic.Critical_Nodes,
      timing_data, sessions_structure, max_threads_limit = 8)
    val min_start = graph.node_depth(x => pth.best_times(x).ms)

    val hol_time = Time.ms(min_start("HOL")) + pth.best_time("HOL")
    args.progress.echo("HOL: " + hol_time.seconds)

    def is_afp(session: String): String = {
      if (benchmark_sessions.contains(session)) "Distribution"
      else if (sessions_structure(session).groups.contains("AFP")) "AFP" else "Distribution"
    }

    def is_pure_succ(session: String): String = {
      if (benchmark_sessions.contains(session)) "Pure"
      else if (graph.imm_preds(session).toList == List("Pure")) "Pure" else "Other"
    }

    val build_history0 =
      (for {
        log_name <- log_names(args.log_db)
        build_info = Build_Log.private_data.read_build_info(args.log_db, log_name)
        session_name <- build_info.finished_sessions
        session = build_info.sessions(session_name)
        hostname <- session.hostname
        host <- host_infos.hosts.find(_.name == hostname)
        threads <- session.threads
        if !session.timing.is_zero
      } yield ((session_name, threads), (host, session.timing))).toList

    val build_history_benchmark =
      if (config.data) {
        (for {
          log_name <- log_names(args.benchmark_db)
          build_info = Build_Log.private_data.read_build_info(args.benchmark_db, log_name)
          session_name <- build_info.finished_sessions
          session = build_info.sessions(session_name)
          hostname <- session.hostname
          host <- host_infos.hosts.find(_.name == hostname)
          threads <- session.threads
          if !session.timing.is_zero
        } yield ((session_name, threads), (host, session.timing))).toList
      } else Nil

    val build_history = build_history0 ::: build_history_benchmark

    def accumulated_time(job: String): Time = {
      if (benchmark_sessions.contains(job))
        pth.best_time("Pure") + build_history_benchmark.filter(_._1._1 == job).map(_._2._2.elapsed).minBy(_.ms)
      else
        Time.ms(min_start(job)) + timing_data.estimate(job, pth.ordered_hosts.last.name, 1)
    }

    def time_factor(time1: Time, time2: Time): Double = time2.ms.toDouble / time1.ms

    val measured_factors_list =
      (for {
        ((session_name, threads), config_timings) <- build_history.groupMap(_._1)(_._2).iterator
        host_times = config_timings.groupMap(_._1)(_._2).view.mapValues(median_time).toMap

        (host1, time1) <- host_times
        (host2, time2) <- host_times
        if host1 != host2
      } yield ((host1, host2), ((session_name, threads), time_factor(time1, time2)))).toList
    val measured_factors = measured_factors_list.groupMap(_._1)(_._2)

    // calc per session (1t?), and for all
    val host_time_mean = measured_factors.view.mapValues(mes => mean(mes.map(_._2))).toMap

    val errors =
      for {
        (hosts, config_factors) <- measured_factors.toList
        if config_factors.size > 1
        ((session, threads), factor) <- config_factors
        if threads == 1
        actual = host_time_mean(hosts)
      } yield (session, (actual - factor) * (actual - factor))

    val res =
      for {
        (session, errors) <- errors.groupMap(_._1)(_._2).toList
        if errors.size >= 10 // require at least 10 data points
        time = accumulated_time(session)
      } yield CSV.Record(session.replace("_", " "), is_afp(session), is_pure_succ(session), time.seconds, mean(errors))

    CSV.File("host_factors_error", List("session", "is_in", "based_on", "time", "error"), res).write(generated)

    hol_time.seconds.toString
  })


  /** analyze speedup **/

  val interpolate_tool = analysis_tool("thread_curves",
  { args =>
    val host_infos = args.host_infos2()
    val unit = Time.ms(1)
    val timing_data = Build_Schedule2.Timing_Data.load(host_infos, args.log_db, Time.ms(1))

    val (errors, errors1) =
      (for {
        (job, facet) <- timing_data.facet.by_job.toList
        (host, facet) <- facet.by_hostname.toList
        if facet.size > 1
        (threads, facet1) <- facet.by_threads.toList
      } yield {
        val elems = facet.results.filter(_.threads != threads)
        val timing_data = new Build_Schedule2.Timing_Data(new Build_Schedule2.Timing_Data.Facet(elems), host_infos, unit)
        val timing_data1 = new Build_Schedule2.Timing_Data(new Build_Schedule2.Timing_Data.Facet(elems), host_infos, unit, false)
        val actual = facet1.median_time
        val predicted = timing_data.estimate(job, host, threads).scale(1.0 / timing_data.FACTOR_NO_THREADS_SAME_MACHINE)
        val predicted1 = timing_data1.estimate(job, host, threads).scale(1.0 / timing_data.FACTOR_NO_THREADS_SAME_MACHINE)
        val error = (actual.ms - predicted.ms).abs.toDouble / actual.ms
        val error1 = (actual.ms - predicted1.ms).abs.toDouble / actual.ms
        (error, error1)
      }).unzip

    // not enough
    for {
      (job, facet) <- timing_data.facet.by_job.toList
      (host, facet) <- facet.by_hostname.toList
      if facet.size > 2 && facet.by_threads.contains(1)
      time_1 = facet.by_threads(1).median_time
      (threads, facet1) <- facet.by_threads.toList
    } yield CSV.Record(job + "/" + host, threads, facet1.median_time.ms.toDouble / time_1.ms)

    def read_line(line: String): (String, Int, Time, Time) =
      space_explode(',', line) match {
        case List(cpu, threads, time, cputime) =>
          (cpu, Value.Int.parse(threads), Time.seconds(Value.Int.parse(time)),
            Time.seconds(Value.Int.parse(cputime)))
        case _ => error("Invalid line: " + line)
      }

    def get(lines: List[(String, Int, Time, Time)], i: Int = 0): List[CSV.Record] =
      lines match {
        case Nil => Nil
        case (elem @ (cpu, 1, time1, _)) :: lines =>
          val (elems0, rest) = lines.span((cpu1, threads1, _, _) => cpu1 == cpu && threads1 > 1)
          val elems = elem :: elems0
          val by_threads = elems.groupMap(_._2)(_._3).view.mapValues(_.map(t => Timing(t, t, t))).mapValues(median_time).toMap
          val records =
            if (by_threads.size < 3) Nil
            else by_threads.toList.map((threads, time) =>
              CSV.Record(i, threads, by_threads(1).ms / time.ms.toDouble))

         records ::: get(rest, i+1)
        case _ :: rest => get(rest, i)
      }

    val lines = split_lines(File.read(data + Path.basic("build_data.csv"))).drop(1)
    val points = get(lines.map(read_line))

    CSV.File("thread_curves", List("job", "threads", "factor"), points).write(generated)

    args.progress.echo("MRE: " + mean(errors) + ", MRE (relu only): " + mean(errors1))
  })


  /** scheduling **/

  case class Problem(
    name: String,
    afp: Boolean,
    selection: Sessions.Selection,
    hosts: String,
    unit: Time = Time.ms(1),
    min_time: Time = Time.zero)

  def cluster = "cluster.ls21old"

  def all_problems = List(
    Problem("3_AFP", true, Sessions.Selection(all_sessions = true,
      exclude_session_groups = List("slow")), cluster),
  ).map(problem => problem.name -> problem).toMap

  val compare_pth_params_tool = schedule_tool("compare_pth_params",
    List("is_criticals", "parallel_threads", "machine_splits", "timing"),
    List(
      Schedule_Config("3_AFP", ()),
    ), { (problem, data, args, build_args, schedule_args) =>
      val sessions_structure = schedule_args.build_context.sessions_structure
      val timing_data = schedule_args.timing_data2

      val is_criticals =
        List(
          Build_Schedule2.Path_Time_Heuristic.Absolute_Time(Time.minutes(1)),
          Build_Schedule2.Path_Time_Heuristic.Absolute_Time(Time.minutes(5)),
          Build_Schedule2.Path_Time_Heuristic.Absolute_Time(Time.minutes(10)),
          Build_Schedule2.Path_Time_Heuristic.Absolute_Time(Time.minutes(15)),
          Build_Schedule2.Path_Time_Heuristic.Absolute_Time(Time.minutes(20)),
          Build_Schedule2.Path_Time_Heuristic.Absolute_Time(Time.minutes(25)),
          Build_Schedule2.Path_Time_Heuristic.Absolute_Time(Time.minutes(30)),
          Build_Schedule2.Path_Time_Heuristic.Relative_Time(0.9),
          Build_Schedule2.Path_Time_Heuristic.Relative_Time(0.75),
          Build_Schedule2.Path_Time_Heuristic.Relative_Time(0.5),
          Build_Schedule2.Path_Time_Heuristic.Relative_Time(0.25),
        )
      val parallel_threads =
        List(
          new Build_Schedule2.Path_Time_Heuristic.Fixed_Thread(1) { override def toString: String = "\\_→1" },
          new Build_Schedule2.Path_Time_Heuristic.Fixed_Thread(4) { override def toString: String = "\\_→4" },
          new Build_Schedule2.Path_Time_Heuristic.Fixed_Thread(8) { override def toString: String = "\\_→8" },
          new Build_Schedule2.Path_Time_Heuristic.Time_Based_Threads({
            case time if time < Time.minutes(1) => 1
            case _ => 8
          }) { override def toString: String = "1→1 \\_→8" },
          new Build_Schedule2.Path_Time_Heuristic.Time_Based_Threads({
            case time if time < Time.minutes(1) => 1
            case time if time < Time.minutes(5) => 4
            case _ => 8
          }) { override def toString: String = "1→1 5→4 \\_→8" },
          new Build_Schedule2.Path_Time_Heuristic.Time_Based_Threads({
            case time if time < Time.minutes(1) => 1
            case time if time < Time.minutes(5) => 2
            case time if time < Time.minutes(10) => 4
            case _ => 8
          }) { override def toString: String = "1→1 5→2 10→4 \\_→8" },
           new Build_Schedule2.Path_Time_Heuristic.Time_Based_Threads({
            case time if time < Time.minutes(2) => 1
            case _ => 8
          }) { override def toString: String = "2→1 \\_→8" },
          new Build_Schedule2.Path_Time_Heuristic.Time_Based_Threads({
            case time if time < Time.minutes(2) => 1
            case time if time < Time.minutes(10) => 4
            case _ => 8
          }) { override def toString: String = "2→1 10→4 \\_→8" },
          new Build_Schedule2.Path_Time_Heuristic.Time_Based_Threads({
            case time if time < Time.minutes(2) => 1
            case time if time < Time.minutes(10) => 2
            case time if time < Time.minutes(20) => 4
            case _ => 8
          }) { override def toString: String = "2→1 10→2 20→4 \\_→8" },
        )
      val machine_splits =
        List(
          Build_Schedule2.Path_Time_Heuristic.Critical_Nodes,
          Build_Schedule2.Path_Time_Heuristic.Fixed_Fraction(0.1),
          Build_Schedule2.Path_Time_Heuristic.Fixed_Fraction(0.3),
          Build_Schedule2.Path_Time_Heuristic.Fixed_Fraction(0.5),
          Build_Schedule2.Path_Time_Heuristic.Host_Speed(0.5),
          Build_Schedule2.Path_Time_Heuristic.Host_Speed(0.7),
          Build_Schedule2.Path_Time_Heuristic.Host_Speed(0.9),
        )

      def calc(
        config: (Build_Schedule2.Path_Time_Heuristic.Critical_Criterion, Build_Schedule2.Path_Time_Heuristic.Parallel_Strategy, Build_Schedule2.Path_Time_Heuristic.Host_Criterion)
      ): List[Any] = {
        val heuristic = Build_Schedule2.Path_Time_Heuristic(config._1, config._2, config._3, timing_data, sessions_structure)
        val scheduler =
          Build_Schedule2.Generation_Scheme(heuristic, schedule_args.timing_data2, schedule_args.build_context.build_uuid)
        val time = scheduler.schedule(schedule_args.build_state).duration
        args.progress.echo(schedule_args.name + " - " + scheduler.priority_rule.toString + ": " + time.message_hms)
        List(config._1.toString, config._2.toString, config._3.toString, time.minutes)
      }

      Par_List.map(calc, (for {
        is_critical <- is_criticals
        parallel <- parallel_threads
        machine_split <- machine_splits
      } yield (is_critical, parallel, machine_split)))
    })
}

class Experiment_Tools extends Isabelle_Scala_Tools(Build_Analysis.tools: _*)