pipeline {
    agent any

    options {
        timestamps()
        disableConcurrentBuilds()
        buildDiscarder(logRotator(numToKeepStr: '30'))
    }

    parameters {
        string(name: 'COREFILE_CONFIG_ID', defaultValue: 'coredns-ecs-corefile', description: 'Jenkins Managed File ID for Corefile')
        string(name: 'TARGET_OS', defaultValue: 'linux', description: 'Target OS for release package')
        string(name: 'TARGET_ARCH', defaultValue: 'amd64', description: 'Target architecture for release package')
        string(name: 'RELEASE_TAG', defaultValue: '', description: 'Optional release tag (empty = Makefile default)')
        booleanParam(name: 'DEPLOY_ENABLED', defaultValue: false, description: 'Deploy artifact to remote host')
        string(name: 'DEPLOY_CREDENTIALS_ID', defaultValue: 'coredns-ecs-ssh', description: 'Jenkins SSH credentials ID')
        string(name: 'DEPLOY_HOST', defaultValue: '', description: 'Remote host for deployment')
        string(name: 'DEPLOY_PORT', defaultValue: '22', description: 'Remote SSH port')
        string(name: 'DEPLOY_USER', defaultValue: 'root', description: 'Remote SSH user')
        string(name: 'DEPLOY_DIR', defaultValue: '/opt/coredns-ecs/releases', description: 'Remote directory to upload artifact')
        booleanParam(name: 'RUN_REMOTE_INSTALL', defaultValue: true, description: 'Run make install and restart service on remote host')
        booleanParam(name: 'USE_SUDO', defaultValue: true, description: 'Use sudo for remote install and restart')
        string(name: 'REMOTE_SERVICE', defaultValue: 'coredns-ecs', description: 'Systemd service name to restart')
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
                sh 'rm -f ./*.tar.gz SHA256SUMS'
            }
        }

        stage('Inject Corefile From Jenkins') {
            steps {
                sh 'mkdir -p jenkins'
                configFileProvider([configFile(fileId: params.COREFILE_CONFIG_ID, targetLocation: 'jenkins/Corefile')]) {
                    sh 'test -s jenkins/Corefile'
                }
            }
        }

        stage('Build Release Package') {
            steps {
                script {
                    def tagArg = params.RELEASE_TAG?.trim() ? "RELEASE_TAG=${params.RELEASE_TAG.trim()}" : ''
                    sh """
                        set -e
                        make release-package \\
                          COREFILE_SRC=jenkins/Corefile \\
                          TARGET_OS=${params.TARGET_OS} \\
                          TARGET_ARCH=${params.TARGET_ARCH} \\
                          ${tagArg}
                        make release-checksum \\
                          COREFILE_SRC=jenkins/Corefile \\
                          TARGET_OS=${params.TARGET_OS} \\
                          TARGET_ARCH=${params.TARGET_ARCH} \\
                          ${tagArg}
                        shasum -a 256 *.tar.gz > SHA256SUMS
                    """
                    env.RELEASE_TAR = sh(script: "set -e; ls -1 *.tar.gz", returnStdout: true).trim().split('\\n')[0]
                }
            }
        }

        stage('Deploy To Remote') {
            when {
                expression { params.DEPLOY_ENABLED }
            }
            steps {
                script {
                    if (!params.DEPLOY_HOST?.trim()) {
                        error('DEPLOY_HOST is required when DEPLOY_ENABLED=true')
                    }
                }
                sshagent(credentials: [params.DEPLOY_CREDENTIALS_ID]) {
                    sh """
                        set -euo pipefail
                        ARTIFACT_TAR='${env.RELEASE_TAR}'
                        SSH_TARGET='${params.DEPLOY_USER}@${params.DEPLOY_HOST}'
                        SSH_PORT='${params.DEPLOY_PORT}'

                        ssh -p "$SSH_PORT" -o StrictHostKeyChecking=accept-new "$SSH_TARGET" "mkdir -p '${params.DEPLOY_DIR}'"
                        scp -P "$SSH_PORT" -o StrictHostKeyChecking=accept-new "$ARTIFACT_TAR" "$SSH_TARGET:${params.DEPLOY_DIR}/"

                        ssh -p "$SSH_PORT" -o StrictHostKeyChecking=accept-new "$SSH_TARGET" \
                          "DEPLOY_DIR='${params.DEPLOY_DIR}' ARTIFACT_TAR='$ARTIFACT_TAR' RUN_REMOTE_INSTALL='${params.RUN_REMOTE_INSTALL}' USE_SUDO='${params.USE_SUDO}' REMOTE_SERVICE='${params.REMOTE_SERVICE}' bash -s" <<'EOF'
                        set -euo pipefail
                        cd "$DEPLOY_DIR"
                        tar -xzf "$ARTIFACT_TAR"
                        RELEASE_DIR="$(tar -tzf "$ARTIFACT_TAR" | cut -d/ -f1 | sed -n '1p')"
                        cd "$RELEASE_DIR"

                        if [ "$RUN_REMOTE_INSTALL" = "true" ]; then
                          if [ "$USE_SUDO" = "true" ]; then
                            SUDO_CMD='sudo'
                          else
                            SUDO_CMD=''
                          fi

                          $SUDO_CMD make install COREFILE_SRC=Corefile
                          $SUDO_CMD systemctl restart "$REMOTE_SERVICE"
                          $SUDO_CMD systemctl status "$REMOTE_SERVICE" --no-pager || true
                        fi
                        EOF
                    """
                }
            }
        }

        stage('Archive Artifacts') {
            steps {
                archiveArtifacts artifacts: '*.tar.gz,SHA256SUMS', fingerprint: true, allowEmptyArchive: true
                archiveArtifacts artifacts: 'release/**', fingerprint: true, allowEmptyArchive: true
            }
        }
    }
}
