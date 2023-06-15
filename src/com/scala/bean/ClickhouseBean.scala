package bean

import scala.beans.BeanProperty

class ClickhouseBean extends Serializable {
  @BeanProperty var day_of_year: String = _
  @BeanProperty var processTime: String = _
  @BeanProperty var monitoring_type: String = _
  @BeanProperty var monitoring_values: String = _
}
