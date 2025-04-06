const apps = [{
    name: `FMDISQ`,
    script: 'index.js',
    instances: 1,
    autorestart: true,
    exec_mode: 'fork',
    watch: false,
    error_file: 'log/err.log',
    out_file: 'log/out.log',
    log_file: 'log/log.log',
    merge_logs: true,
    pmx: false,
}];

module.exports = {
	apps
}