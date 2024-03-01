package ru.pandahouse.eulerdb.configuration;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.internal.storage.commitgraph.CommitGraph;
import org.eclipse.jgit.internal.storage.commitgraph.CommitGraphLoader;
import org.eclipse.jgit.internal.storage.commitgraph.CommitGraphWriter;
import org.eclipse.jgit.internal.storage.commitgraph.GraphCommits;
import org.eclipse.jgit.lib.NullProgressMonitor;
import org.eclipse.jgit.lib.ObjectId;
import org.eclipse.jgit.lib.ProgressMonitor;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.revwalk.RevWalk;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class JGitConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(JGitConfig.class);
    private final Git git;
    private final Repository repository;

    private CommitGraph commitGraph;
    private final String repoDir = "/Users/alexunderit/IdeaProjects/RepoProjects/git/.git";
    private static final String cgfRef = "/Users/alexunderit/IdeaProjects/RepoProjects/git/.git/objects/info/commit-graph";

    public JGitConfig(){
        repository = createRepo(repoDir);
        git = new Git(repository);
        commitGraph = readCommitGraphFile(cgfRef);
    }

    private Repository createRepo(String repoDir){
        try(Repository repo = new FileRepositoryBuilder()
                .setGitDir(new File(repoDir))
                .build();
        ){
            LOGGER.info("Create repo with directory ref: {}", repoDir);
            LOGGER.info("Current HEAD pointing branch is: {}", repo.getBranch());
            return repo;
        } catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    private CommitGraph createCommitGraphFile() throws IOException, GitAPIException {
        CommitGraph commitGraph;
        RevWalk revWalk = new RevWalk(repository);

        //Начальный коммит, с которого будет строиться коммит-граф файл
        Set<ObjectId> objectIdSet = new HashSet<>();
        objectIdSet.add(ObjectId.fromString("c875e0b8e036c12cfbf6531962108a063c7a821c"));

        ProgressMonitor m = NullProgressMonitor.INSTANCE;
        GraphCommits graphCommits = GraphCommits.fromWalk(m, objectIdSet,revWalk);
        CommitGraphWriter commitGraphWriter = new CommitGraphWriter(graphCommits);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        commitGraphWriter.write(m, os);

        InputStream inputStream = new ByteArrayInputStream(os.toByteArray());
        commitGraph = CommitGraphLoader.read(inputStream);

        LOGGER.info("Successfully created Commit-graph file");
        return commitGraph;
    }

    private CommitGraph readCommitGraphFile(String ref) {
        try{
            File cgFile = new File(ref);
            if(cgFile.exists()){
                return CommitGraphLoader.open(cgFile);
            } else{
                return CommitGraph.EMPTY;
            }
        } catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    public static void getComitGraphFileInfo(CommitGraph commitGraph, String commitHash){
        ObjectId testCommit = ObjectId.fromString(commitHash);
        int position = commitGraph.findGraphPosition(testCommit);
        LOGGER.info("total count of commits in commit-graph file: {}", commitGraph.getCommitCnt());
        LOGGER.info("Time for commit {}: {}", commitHash, commitGraph.getCommitData(position).getCommitTime());
        LOGGER.info("Generation for commit {}: {}", commitHash, commitGraph.getCommitData(position).getGeneration());
        LOGGER.info("Parents for commit {}: {}", commitHash,
                Arrays.stream(commitGraph.getCommitData(position).getParents())
                        .mapToObj(i -> ObjectId.toString(commitGraph.getObjectId(i)))
                        .collect(Collectors.toList()));
        LOGGER.info("Tree for commit {}: {}", commitHash, commitGraph.getCommitData(position).getTree());
        LOGGER.info("ObjectId start: {}, ObjectId end: {}", ObjectId.toString(testCommit) ,ObjectId.toString(commitGraph.getObjectId(position)));
    }

    public CommitGraph getCommitGraphFile() throws IOException, GitAPIException{
        return commitGraph.equals(CommitGraph.EMPTY) ? createCommitGraphFile() : commitGraph;
    }

}
