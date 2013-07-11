Gem::Specification.new do |spec|
  spec.name     = "fluent-plugin-vertica"
  spec.version  = File.read("VERSION").strip
  spec.authors  = ["Erik Selin"]
  spec.email    = ["erik.selin@ifelsestudio.com"]
  spec.homepage = "http://tyro89.github.com/fluent-plugin-vertica"

  spec.summary = "Fluentd output plugin for Vertica."

  spec.required_ruby_version = '>= 1.9.1'

  spec.add_dependency 'fluentd', '~> 0.10.35'
  spec.add_dependency 'vertica'

  spec.files = Dir['LICENSE', 'README.md', '{lib, test}/**/*']
end
