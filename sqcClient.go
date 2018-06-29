package main

import (
  "fmt"
  "os"
  "net"
  "log"
  "bytes"
  "io"
  "bufio"
  "time"
  "flag"
  "regexp"
  "strconv"
  "strings"
)

func parseNodelist(ss string)(string){
  st := strings.Split(ss,",")
  parsed := ""
  prefix := ""

  for _,s := range st {
    na := ""
    nu := ""
    b := strings.Contains(s,"[")

    if !b {
      re := regexp.MustCompile("^[A-Za-z]")
      if re.MatchString(s) {
        //return "",s+" "
        prefix = ""
        parsed += s+" "
        continue
      }
    }
  
    for _,t := range s {
      if t==']' { continue }
      if b {
        if t=='[' { b = false; continue }
        na += string(t)
      } else {
        nu += string(t)
      }
    }

    if na == "" { na = prefix }
    if strings.Contains(nu,"-") {
      m := strings.Split(nu,"-")
      if len(m) == 2 {
        nn := ""
        a,_ := strconv.Atoi(m[0])
        b,_ := strconv.Atoi(m[1])
        c := strconv.Itoa(a)
        d := len(m[0])-len(c)  // how many 0's to pad
        for i:=a; i<=b; i++ {
          nn += na
          for j:=0; j<d; j++ {
            nn += "0"
          }
          nn += strconv.Itoa(i)+" "
        }
        prefix = na
        parsed += nn
      }
    } else {
      prefix = na
      parsed += na+nu+" "
    }
  }
  return parsed
}

func readAll(conn net.Conn)([]byte,error){
  defer conn.Close()
  r := bytes.NewBuffer(nil)
  var b [8192]byte
  for {
    n,err := conn.Read(b[0:])
    r.Write(b[0:n])
    if err != nil {
      if err == io.EOF { break }
      return nil,err
    }
  }
  return r.Bytes(),nil
}

func fetchJobs(jobs *[][]string, srv string){
  conn,err := net.Dial("tcp", srv)
  if err != nil {
    log.Fatal(err)
  }
  defer conn.Close()
  conn.SetReadDeadline(time.Now().Add(5*time.Second))

  b,err := readAll(conn)
  if err != nil {
    fmt.Fprintf(os.Stderr, "read error: %s\n", err.Error())
  }

  scanner := bufio.NewScanner(strings.NewReader(string(b)))
  for scanner.Scan(){
    s := strings.Split(scanner.Text(),"<|>")
    *jobs = append(*jobs,s)
  }
}

func key2pos(b byte)(int){
  // steps:   jkey = []byte{'A','M','N','P','S','U','b','i','j','l','u'}
  if stepFlag {
    switch b {
    case 'A':  return 0
    case 'M':  return 1
    case 'N':  return 2
    case 'P':  return 3
    case 'S':  return 4
    case 'U':  return 5
    case 'b':  return 6
    case 'i':  return 7
    case 'j':  return 8
    case 'l':  return 9
    case 'u':  return 10
    }
    return -1
  }

  i := 0
  if b <= 'Z' {
    i = int(b - 'A')
    return i
  } else {
    i = int(b - 'a') + 26
    return i
  }
  return -1
}

func parseFormat(s string)(int,string,int,bool){
  r := regexp.MustCompile("[0-9]+")
  if len(s) == 0 { return 0,"",-1,false }
  a := r.Split(s,-1)
  b := r.FindString(s)
  n := 0
  if b != "" {
    var e error
    n,e = strconv.Atoi(b)
    if e!=nil { return 0,"",-1,false}
  }
  rj := false
  if s[0] == '.' { rj = true }
  if n != 0 {
    t := ""
    for i:=1; i<len(a[1]); i++ { t += string(a[1][i]) }
    return n,t,key2pos(a[1][0]),rj
  } else {
    t := ""
    for i:=1; i<len(a[0]); i++ { t += string(a[0][i]) }
    return n,t,key2pos(a[0][0]),rj
  }
  return 0,"",-1,false
}

type jformat struct {
  space  int
  trail  string
  pos    int
  rightJ bool
}

func outputFormat(s string)([]jformat){
  var jfs []jformat
  ss := strings.Split(s,"%")
  ss = ss[1:]   // skip first since it's empty
  for _,u := range ss {
    var j jformat
    j.space,j.trail,j.pos,j.rightJ = parseFormat(u)
      jfs = append(jfs,j)
  }
  return jfs
}

func printOutput(jobs *[][]string, s string)(string){
  jfs := outputFormat(s)
  out := ""
  for _,j := range *jobs {
    for _,u := range jfs {
      n, t, p, r := u.space, u.trail, u.pos, u.rightJ
      if p < 0 { continue }
      o := j[p]
      if n != 0 {
        m := len(o)
        if m > n {
          o = o[0:n]
        } else {
          for i:=0; i<n-m; i++ {
            if r {
              o = " "+o
            } else {
              o = o + " "
            }
          }
        }
      }
      out += o+t
    }
    if out != "" {
      out += "\n"
    }
  }
  return out
}

var longFlag,stepFlag,noheaderFlag bool
var formatFlag,accountFlag,jobFlag,nameFlag,partitionFlag,qosFlag,reservationFlag,nodelistFlag,statesFlag,userFlag,sortFlag string
func init(){
  flag.BoolVar(&stepFlag, "step", false, "job steps")
  flag.BoolVar(&stepFlag, "s",    false, "job steps")
  flag.BoolVar(&longFlag, "long", false, "long format")
  flag.BoolVar(&longFlag, "l",    false, "long format")
  flag.BoolVar(&noheaderFlag, "noheader", false, "long format")
  flag.BoolVar(&noheaderFlag, "h",    false, "long format")
  flag.StringVar(&formatFlag, "format", "", "output format")
  flag.StringVar(&formatFlag, "o",      "", "output format")
  flag.StringVar(&accountFlag, "account", "", "select accounts to print")
  flag.StringVar(&accountFlag, "A",      "", "select accounts to print")
  flag.StringVar(&jobFlag, "job", "", "select jobid to print")
  flag.StringVar(&jobFlag, "j",      "", "select jobid to print")
  flag.StringVar(&nameFlag, "name", "", "select jobname to print")
  flag.StringVar(&nameFlag, "n",      "", "select jobname to print")
  flag.StringVar(&partitionFlag, "partition", "", "select partition to print")
  flag.StringVar(&partitionFlag, "p",      "", "select partition to print")
  flag.StringVar(&qosFlag, "qos", "", "select qos to print")
  flag.StringVar(&qosFlag, "q",      "", "select qos to print")
  flag.StringVar(&reservationFlag, "reservation", "", "select reservation to print")
  flag.StringVar(&reservationFlag, "R",      "", "select reservation to print")
  flag.StringVar(&nodelistFlag, "nodelist", "", "select nodelist to print")
  flag.StringVar(&nodelistFlag, "w",      "", "select nodelist to print")
  flag.StringVar(&statesFlag, "states", "", "select states to print")
  flag.StringVar(&statesFlag, "t",      "", "select states to print")
  flag.StringVar(&userFlag, "user", "", "select user to print")
  flag.StringVar(&userFlag, "u",      "", "select user to print")
  flag.StringVar(&sortFlag, "sort", "", "sort list")
  flag.StringVar(&sortFlag, "S",      "", "sort list")
}


func selectJobs(jobs *[][]string, k byte, ft string)([][]string){
  var jj [][]string
  p := key2pos(k)
  jj = append(jj,(*jobs)[0])
  s := strings.Split(ft,",")
  for _,m := range *jobs {
    for _,t := range s {
      u := m[p]
      if k == 'N' {  // matching nodelist
        u = parseNodelist(u)
        if strings.Contains(u,t) {
          jj = append(jj,m)
          break
        }
      } else {
        if u == t {
          jj = append(jj,m)
          break
        }
      }
    }
  }
  return jj
}

func main(){

  // move away -s argument otherwise break flag arguments
  jstep := ""
  var modArgs []string
  for i:=0; i<len(os.Args); i++ {
    modArgs = append(modArgs,os.Args[i])
    if strings.Contains(os.Args[i],"-s") && len(os.Args) - i > 1 && ! strings.Contains(os.Args[i+1],"-") {
      jstep = os.Args[i+1]
      i ++
    } 
  }
  os.Args = modArgs

  flag.Parse()
  if len(flag.Args()) > 0 {
    fmt.Fprintf(os.Stderr, "invalid command line options: %v\n",flag.Args())
    os.Exit(1)
  }

  var jobs [][]string

  sqcServer := "localhost"
  if stepFlag {
    sqcServer += ":6601"
  } else {
    sqcServer += ":6600"
  }
  fetchJobs(&jobs,sqcServer)
  if len(jobs) < 1 {
    fmt.Fprintf(os.Stderr, "no jobs fetched.\n")
    os.Exit(1)
  }

  format := "%.18i %.9P %.8j %.8u %.2t %.10M %.6D %R"
  if longFlag { format = "%.18i %.9P %.8j %.8u %.8T %.10M %.9l %.6D %R" }
  if stepFlag { format = "%.15i %.8j %.9P %.8u %.9M %N" }
  if formatFlag != "" {
    format = formatFlag
  }

  filters := []string{jstep,accountFlag,jobFlag,nameFlag,partitionFlag,qosFlag,reservationFlag,nodelistFlag}
  columns := []byte{'i','a','i','j','P','q','v','N'}
  for i,_ := range filters {
    if filters[i] != "" {
      jobs = selectJobs(&jobs,columns[i],filters[i])
    }
  }

  if statesFlag != "" {
    if len(statesFlag) <=2 {
      jobs = selectJobs(&jobs,'t',statesFlag)
    } else {
      jobs = selectJobs(&jobs,'T',statesFlag)
    }
  }

  if userFlag != "" {
    re := regexp.MustCompile("^[0-9]+")
    if re.MatchString(userFlag) {
      jobs = selectJobs(&jobs,'U',userFlag)  //numeric uid
    } else {
      jobs = selectJobs(&jobs,'u',userFlag)  //user name
    }
  }

  //sort
  var header [][]string
  header = append(header,jobs[0])
  jobs = jobs[1:]

  if sortFlag != "" {
    var lf []lessFunc
    sf := strings.Split(sortFlag,",")
    for _,v := range sf {
      if len(v) > 2 { fmt.Fprintf(os.Stderr, "sort list error, cannot sort: %s\n",v); os.Exit(2) }
      a := true
      b := byte(v[0])
      if v[0] == '-' {
        a = false
        b = byte(v[1])
      }
      i := key2pos(b)
      if i > 0 {
        if a {
          f := incColumn(i)
          lf = append(lf,f)
        } else {
          f := decColumn(i)
          lf = append(lf,f)
        }
      }
    }
    OrderedBy(lf...).Sort(jobs)
  }
 
  if !noheaderFlag {
    fmt.Print(printOutput(&header,format))
  }

  // split output with goroutines, much faster for large output
  if len(jobs) > 120 {
    const SZ = 60
    js := len(jobs)
    n := js/SZ+1
    a := 0
    ss := make([]string,n)
    done := make(chan bool)
    for i:=0; i<n; i++ {
      var j [][]string
      if a+SZ <= js {
        j = jobs[a:a+SZ]
      }else{
        j = jobs[a:]
      }
      go func(ss []string, i int) {
        ss[i] = printOutput(&j,format)
        //fmt.Println(ss[i])
        done <- true
      }(ss,i)
      a += SZ
    }
    for i:=0; i<n; i++ {
      <-done
    }
    for i:=0; i<n; i++ {
      fmt.Print(ss[i])
    }
  } else {
    fmt.Print(printOutput(&jobs,format))
  }
}
