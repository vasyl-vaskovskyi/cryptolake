package com.cryptopanner.verify;

import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(
    name = "cryptopanner-verify",
    mixinStandardHelpOptions = true,
    subcommands = {VerifyCommand.class},
    description = "CryptoPanner audit / integrity CLI (skeleton).")
public final class Main implements Runnable {

  public static void main(String[] args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }

  @Override
  public void run() {
    new CommandLine(this).usage(System.out);
  }
}
