package diskv

type Server struct {
	gid        int64
	masters    []string
	replicas   []string
	me         int
	dir        string
	restart    bool
	unreliable bool
}

func (s *Server) Setunreliable(unreliable bool) {
	s.unreliable = unreliable
}

func StartServer(gid int64, masters []string, replicas []string, me int, dir string, restart bool) *Server {
	return &Server{
		gid:      gid,
		masters:  masters,
		replicas: replicas,
		me:       me,
		dir:      dir,
		restart:  restart,
	}
}
