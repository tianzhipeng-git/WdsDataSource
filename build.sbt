ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "1.0-SNAPSHOT"
ThisBuild / organization := "io.github.tianzhipeng-git"

enablePlugins(AssemblyPlugin)
Compile / packageBin := (Compile / packageBin).dependsOn(assembly).value

lazy val root = (project in file("."))
  .settings(
    name := "WdsDataSource",
    
    // 依赖项配置
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-library" % scalaVersion.value,
      "org.scalatest" %% "scalatest" % "3.2.15" % Test,
      "org.scalatest" %% "scalatest-funsuite" % "3.2.15" % Test,
      "org.apache.spark" %% "spark-core" % "3.3.2" % Provided,
      "org.apache.spark" %% "spark-sql" % "3.3.2" % Provided,
      "org.apache.hadoop" % "hadoop-common" % "3.3.2" % Provided,
      "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.2" % Provided,
      "com.typesafe.play" %% "play-json" % "2.9.2" % Test,
    ),

    // Java 和 Scala 编译选项
    javacOptions ++= Seq(
      "-source", "1.8",
      "-target", "1.8",
      "-Xlint:all,-serial,-path,-try"
    ),
    
    scalacOptions ++= Seq(
      "-unchecked",
      "-deprecation",
      "-feature",
      "-explaintypes",
      "-Yno-adapted-args",
      "-target:jvm-1.8"
    ),

    // 源代码目录配置
    Compile / scalaSource := baseDirectory.value / "src/main/scala",
    Test / scalaSource := baseDirectory.value / "src/test/scala",

    // assembly 配置（对应 maven-shade-plugin）
    assembly / assemblyJarName := s"${name.value}-${version.value}.jar",
    assembly / mainClass := Some("com.xx.yy.generate.RandomDataGen1"),
    assembly / assemblyExcludedJars := {
      val cp = (assembly / fullClasspath).value
      cp filter { f =>
        f.data.getName.startsWith("servlet-api-") ||
        f.data.getName.startsWith("hadoop-") ||
        f.data.getName.startsWith("spark-") ||
        f.data.getName.startsWith("avro-") ||
        f.data.getName.startsWith("parquet-") ||
        f.data.getName.startsWith("scala-")
      }
    },
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", xs @ _*) =>
        xs match {
          case "MANIFEST.MF" :: Nil => MergeStrategy.discard
          case _ if xs.exists(_.toLowerCase.endsWith(".sf")) => MergeStrategy.discard
          case _ if xs.exists(_.toLowerCase.endsWith(".dsa")) => MergeStrategy.discard
          case _ if xs.exists(_.toLowerCase.endsWith(".rsa")) => MergeStrategy.discard
          case _ => MergeStrategy.first
        }
      case _ => MergeStrategy.first
    }
  ) 