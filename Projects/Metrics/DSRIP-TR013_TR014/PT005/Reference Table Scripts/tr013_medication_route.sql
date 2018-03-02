SET DEFINE OFF;


CREATE TABLE meta_med_route
(
  criterion_id      NUMBER(6),   
  network           CHAR(3 BYTE) DEFAULT 'ALL',
  qualifier         VARCHAR2(30 BYTE) DEFAULT 'NONE',
  value             VARCHAR2(255 BYTE),
  value_description VARCHAR2(255 BYTE),
  route             VARCHAR2(255 BYTE),
  CONSTRAINT meta_med_fk_criteria  FOREIGN KEY (criterion_id,network,qualifier,value) REFERENCES meta_conditions (criterion_id,network,qualifier,value)
);


Insert into meta_med_route(criterion_id,value,value_description,route) values(41,'%omalizumab%', 'ANTIBODY INHIBITOR','subcutaneous');
Insert into meta_med_route(criterion_id,value,value_description,route) values(41,'%Xolair%', 'ANTIBODY INHIBITOR','subcutaneous');


Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Accolate%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Advair Diskus%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Advair HFA%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Aerospan%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%AirDuo RespiClick%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Alvesco HFA%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%ArmonAir RespiClick%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Arnuity Ellipta%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Asmanex HFA%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Asmanex Twisthaler%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%beclomethasone%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Breo Ellipta%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Budesonide%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%budesonide-formoterol%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%ciclesonide CFC free%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Cinqair%', 'TR013 ASTHMA CONTROLLER MEDICATION','intravenous');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%cromolyn%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Cromolyn Sodium%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Difil G%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Difil-G Forte%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Dulera%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%dyphylline%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%dyphylline-guaifenesin%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Ed-Bron G%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Elixophyllin%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Flovent Diskus%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Flovent HFA%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%flunisolide%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%fluticasone%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%fluticasone CFC free%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%fluticasone furoate%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%fluticasone propionate%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Fluticasone-Salmeterol%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%fluticasone-salmeterol CFC free%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%fluticasone-vilanterol%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%formoterol-mometasone%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%guaifenesin-theophylline%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Jay-Phyl%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Lufyllin%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Lufyllin-400%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%mepolizumab%', 'TR013 ASTHMA CONTROLLER MEDICATION','subcutaneous');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%mometasone%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%montelukast%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Montelukast Sodium%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Nucala%', 'TR013 ASTHMA CONTROLLER MEDICATION','subcutaneous');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%omalizumab%', 'TR013 ASTHMA CONTROLLER MEDICATION','subcutaneous');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Pulmicort Flexhaler%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Pulmicort Respules%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Qvar%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Qvar with Dose Counter%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%reslizumab%', 'TR013 ASTHMA CONTROLLER MEDICATION','intravenous');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Singulair%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Symbicort%', 'TR013 ASTHMA CONTROLLER MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Theo-24%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Theophylline%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Theophylline ER%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Theophylline SR%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Xolair%', 'TR013 ASTHMA CONTROLLER MEDICATION','subcutaneous');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Zafirlukast%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Zileuton%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Zyflo%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(44,'%Zyflo CR%', 'TR013 ASTHMA CONTROLLER MEDICATION','oral');


Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Accolate%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Advair Diskus%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Advair HFA%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Aerospan%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%AirDuo RespiClick%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%albuterol%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Alvesco HFA%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%ArmonAir RespiClick 113%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Arnuity Ellipta%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Asmanex HFA%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Asmanex Twisthaler 120 Dose%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%beclomethasone%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Breo Ellipta%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Budesonide%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%budesonide-formoterol%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%ciclesonide CFC free%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%cromolyn%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Cromolyn Sodium%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Difil G%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Difil-G Forte%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Dulera%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%dyphylline%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%dyphylline-guaifenesin%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Ed-Bron G%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Elixophyllin%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Flovent Diskus%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Flovent HFA%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%flunisolide%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%fluticasone%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%fluticasone CFC free%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%fluticasone furoate%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%fluticasone propionate%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Fluticasone-Salmeterol%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%fluticasone-salmeterol CFC free%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%fluticasone-vilanterol%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%formoterol-mometasone%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%guaifenesin-theophylline%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Jay-Phyl%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%levalbuterol%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Levalbuterol Tartrate HFA%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Lufyllin%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Lufyllin-400%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Maxair Autohaler%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%mometasone%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%montelukast%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Montelukast Sodium%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%omalizumab%', 'TR013 OTHER ASTHMA MEDICATION','subcutaneous');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%pirbuterol%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%ProAir HFA%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%ProAir RespiClick%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Proventil HFA%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Pulmicort Flexhaler%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Pulmicort Respules%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Qvar%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Qvar with Dose Counter%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Singulair%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Symbicort%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Theo-24%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Theophylline%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Theophylline ER%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Theophylline SR%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Ventolin HFA%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Xolair%', 'TR013 OTHER ASTHMA MEDICATION','subcutaneous');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Xopenex HFA%', 'TR013 OTHER ASTHMA MEDICATION','inhalation');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Zafirlukast%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Zileuton%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Zyflo%', 'TR013 OTHER ASTHMA MEDICATION','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(45,'%Zyflo CR%', 'TR013 OTHER ASTHMA MEDICATION','oral');


Insert into meta_med_route(criterion_id,value,value_description,route) values(46,'%Accolate%', 'TR013 LEUKOTRIENE MODIFIERS','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(46,'%Montelukast Sodium%', 'TR013 LEUKOTRIENE MODIFIERS','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(46,'%montelukast%', 'TR013 LEUKOTRIENE MODIFIERS','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(46,'%Singulair%', 'TR013 LEUKOTRIENE MODIFIERS','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(46,'%zafirlukast%', 'TR013 LEUKOTRIENE MODIFIERS','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(46,'%zileuton%', 'TR013 LEUKOTRIENE MODIFIERS','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(46,'%Zyflo CR%', 'TR013 LEUKOTRIENE MODIFIERS','oral');
Insert into meta_med_route(criterion_id,value,value_description,route) values(46,'%Zyflo%', 'TR013 LEUKOTRIENE MODIFIERS','oral');


commit;